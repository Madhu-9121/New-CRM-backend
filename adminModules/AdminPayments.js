const { dbPool } = require("../../src/database/mysql")
const { call } = require("../../utils/codeHelper")
const moment = require("moment");
const fs = require('fs');
const e = require("cors");
const { nullCheck } = require("../../database/utils/utilFuncs");
const { count } = require("console");
const { sendMail } = require("../../utils/mailer");
const config = require("../../config");
const { emailEnabledBanks, enabledFinanciersForLC, sleep } = require("../../urlCostants");
const { getCurrentTimeStamp, mysqlTextParse, evaluateCPCommissionPercentBasedOnInvoiceAmount, formatSqlQuery, jsonStr } = require("../../iris_server/utils");
const { getModifiApiToken, getDealInfo } = require("../../src/cronjobs/modifi");
const { getCPListWithCommission, getCPInvoiceDetails } = require("./AdminReporting");

function generateInvoiceSummary(resp, configuration) {
  if ( resp?.commissionFromFinancierDbData?.length) {
    let summaryOfInvoiceCommission = { "totalFinanceAmount": 0, "totalCommissionPercentage": 0, "totalCommissionAmount": 0 }
    for (let index = 0; index < resp.commissionFromFinancierDbData.length; index++) {
      const element = resp.commissionFromFinancierDbData[index];
      let charges = element.invCharges || element.lcCharges
      summaryOfInvoiceCommission["totalFinanceAmount"] += element.contractAmount / 1
      summaryOfInvoiceCommission["totalCommissionPercentage"] += charges.commissionPercentage / 1
      summaryOfInvoiceCommission["totalCommissionAmount"] += (charges.totalCharges * (charges.commissionPercentage / 100))
    }
    summaryOfInvoiceCommission["totalFinanceAmount"] = summaryOfInvoiceCommission["totalFinanceAmount"].toFixed(2)
    summaryOfInvoiceCommission["totalCommissionAmount"] = summaryOfInvoiceCommission["totalCommissionAmount"].toFixed(2)
    summaryOfInvoiceCommission["totalCommissionPercentage"] = (summaryOfInvoiceCommission["totalCommissionPercentage"] / resp.commissionFromFinancierDbData.length).toFixed(2)

    // Creating Invoice data financier wise start
    for (let j = 0; j < resp.commissionFromFinancierDbData.length; j++) {
      const item = resp.commissionFromFinancierDbData[j];
      let charges = item.invCharges || item.lcCharges
      let status = item.invCharges ? item.invoiceCommissionStatus : item.lcCommissionStatus

      if (!summaryOfInvoiceCommission[item.lenderId]) {
        summaryOfInvoiceCommission[item.lenderId] = {
          data: [], item, totalCharges: 0, totalChargesCurrency: 'NA',
          totalCommissionPercentage: 0, commissionPayout: 0,
          billNo: "TRINV" + new Date().getTime(), billCreatedAt: moment().format("YYYY-MM-DD"),
          commissionFrom: configuration.commissionFrom, commissionTo: configuration.commissionTo
        }
      }

      summaryOfInvoiceCommission[item.lenderId]["data"].push([moment(item.applicationCreatedAt).format("YYYY-MM-DD"), item.sellerName, `${(item.invCharges ? 'Invoice Discounting - ' : 'LC Discounting - ')}${item.applicationNo}`, `${item.invoiceDueDate}`, `${item.contractAmount} USD`,
      (charges.totalCharges).toFixed(2) + " USD",
      charges.commissionPercentage,
      ((charges.totalCharges * charges.commissionPercentage) / 100).toFixed(2) + " USD", `${status / 1 == 1 ? "Paid" : "Due"}`,
      `${item.commissionDate ? moment(item.commissionDate).format("YYYY-MM-DD") : "NA"}`])

      summaryOfInvoiceCommission[item.lenderId]["totalCharges"] += charges.totalCharges
      summaryOfInvoiceCommission[item.lenderId]["totalChargesCurrency"] = charges.totalChargesCurrency
      summaryOfInvoiceCommission[item.lenderId]["totalCommissionPercentage"] += charges.commissionPercentage
      summaryOfInvoiceCommission[item.lenderId]["commissionPayout"] += (charges.totalCharges * (charges.commissionPercentage / 100))
    }
    // Creating Invoice data financier wise end
    return summaryOfInvoiceCommission
  }
  else {
    return {}
  }
}

// Cronjob to create financier bills for the invoice which are going to due in current month start
exports.generateInvoiceBills = async () => {
  while (true) {
    try {
      // Is 1st day of month
      let todayDateObj = moment()
      if(todayDateObj.clone().format("DD")/1 == 1){
        let invBanks = await emailEnabledBanks()
        let lcBanks = await enabledFinanciersForLC()
        let finList = invBanks.concat(lcBanks)
        for (let index = 0; index < finList.length; index++) {
          const element = finList[index];
          let query = `SELECT * FROM tbl_invoice_billing WHERE billDate = '${todayDateObj.clone().format("YYYY-MM-DD")}' 
          AND JSON_UNQUOTE(JSON_EXTRACT(details, '$.item.lenderId')) = '${element.id}' `
          let dbRes = await call({ query }, 'makeQuery', 'get');
          if(!dbRes.message.length){
            // Code to raise invoice
            let commissionFrom = todayDateObj.clone().subtract(1,"month").format("YYYY-MM-01")
            let commissionTo = todayDateObj.clone().subtract(1,"month").endOf("month").format("YYYY-MM-DD")
            let apiResp = await this.getAdminPaymentsResport({body: {commissionFrom, commissionTo,"generateInvoice":true,"generateInvoiceForLabel":[element.name],"generateInvoiceForId":[element.id]}})
            // console.log("apiRessssssssssssssssssssssssss", apiResp);
            let summaryOfInvoiceCommission = generateInvoiceSummary(apiResp?.message, {commissionFrom, commissionTo})
            // console.log("summaryOfInvoiceCommissionnnnnnnnnnn", summaryOfInvoiceCommission);
            let invoiceGenerationResp = await this.generateInvoiceForBilling({body: {invoices: Object.values(summaryOfInvoiceCommission)}})
            // console.log("generateInvoiceForBillinggggggggggggggg", invoiceGenerationResp);
          }
          else{
            console.log("Invoice already raised for the lender ", element.name, " for ", todayDateObj);
          }
        }
      }
    } catch (error) {
      console.log("error in generateInvoiceBills", error);
    }
    await sleep(21600000) //Repeat after 6 HRS
  }
}
// Cronjob to create financier bills for the invoice which are going to due in current month end

// At a time only one CP Invoice can be generated
function generateInvoiceSummaryForCP(resp, configuration) {
  if (resp?.commissionFromCPDbData?.length) {
    let summaryOfInvoiceCommission = {
      "totalFinanceAmount": 0, "totalCommissionPercentage": 0, "totalCommissionAmount": 0,
      "CPCommissionPercentage": 0, "totalCPCommissionAmount": 0
    }
    let {selectedCPDetails} = configuration 

    // console.log("selectedCPDetailsselectedCPDetails", selectedCPDetails, resp.commissionFromCPDbData);

    for (let index = 0; index < resp.commissionFromCPDbData.length; index++) {
      const element = resp.commissionFromCPDbData[index];
      let charges = element.invCharges || element.lcCharges
      summaryOfInvoiceCommission["totalFinanceAmount"] += element.contractAmount / 1
      summaryOfInvoiceCommission["totalCommissionPercentage"] += charges.commissionPercentage / 1
      summaryOfInvoiceCommission["totalCommissionAmount"] += (charges.totalCharges * (charges.commissionPercentage / 100))
    }

    summaryOfInvoiceCommission["totalFinanceAmount"] = summaryOfInvoiceCommission["totalFinanceAmount"].toFixed(2)
    summaryOfInvoiceCommission["totalCommissionAmount"] = summaryOfInvoiceCommission["totalCommissionAmount"].toFixed(2)
    summaryOfInvoiceCommission["totalCommissionPercentage"] = (summaryOfInvoiceCommission["totalCommissionPercentage"] / resp.commissionFromCPDbData.length).toFixed(2)

    summaryOfInvoiceCommission["CPCommissionPercentage"] = evaluateCPCommissionPercentBasedOnInvoiceAmount(summaryOfInvoiceCommission["totalFinanceAmount"], selectedCPDetails["commissionCharges"]).toFixed(2)
    summaryOfInvoiceCommission["totalCPCommissionAmount"] = (summaryOfInvoiceCommission["totalCommissionAmount"] * summaryOfInvoiceCommission["CPCommissionPercentage"] / 100).toFixed(2)

    // Creating Invoice data CP wise start
    for (let j = 0; j < resp.commissionFromCPDbData.length; j++) {
      const item = resp.commissionFromCPDbData[j];
      let charges = item.invCharges || item.lcCharges
      let status = item.invCharges ? item.invoiceCommissionStatus : item.lcCommissionStatus

      if (!summaryOfInvoiceCommission[selectedCPDetails["id"]]) {
        summaryOfInvoiceCommission[selectedCPDetails["id"]] = {
          data: [], item, cpId: selectedCPDetails.id, totalCharges: 0, totalChargesCurrency: 'NA',
          totalCommissionPercentage: 0, commissionPayout: 0,
          billNo: "TRINV" + new Date().getTime(), billCreatedAt: moment().format("YYYY-MM-DD"),
          commissionFrom: configuration.commissionFrom, commissionTo: configuration.commissionTo,
          "CPCommissionPercentage": summaryOfInvoiceCommission["CPCommissionPercentage"], "totalCPCommissionAmount": summaryOfInvoiceCommission["totalCPCommissionAmount"]
        }
      }

      summaryOfInvoiceCommission[selectedCPDetails["id"]]["data"].push([moment(item.applicationCreatedAt).format("YYYY-MM-DD"), item.sellerName, `${(item.invCharges ? 'Invoice Discounting - ' : 'LC Discounting - ')}${item.applicationNo}`, `${item.invoiceDueDate}`, `${item.contractAmount} USD`,
      (charges.totalCharges).toFixed(2) + " USD",
      charges.commissionPercentage,
      (charges.totalCharges * (charges.commissionPercentage/100)).toFixed(2) + " USD"])

      summaryOfInvoiceCommission[selectedCPDetails["id"]]["totalCharges"] += charges.totalCharges
      summaryOfInvoiceCommission[selectedCPDetails["id"]]["totalChargesCurrency"] = charges.totalChargesCurrency
      summaryOfInvoiceCommission[selectedCPDetails["id"]]["totalCommissionPercentage"] += charges.commissionPercentage
      summaryOfInvoiceCommission[selectedCPDetails["id"]]["commissionPayout"] += (charges.totalCharges * (charges.commissionPercentage/100))

    }
    // Creating Invoice data CP wise end
    return summaryOfInvoiceCommission
  }
  else {
    return {}
  }
}

// Cronjob to create channel partner bills for the invoice which are going to due in current month start
exports.generateInvoiceBillsForCP = async () => {
  while (true) {
    try {
      // Is 1st day of month
      let todayDateObj = moment()
      if(todayDateObj.clone().format("DD")/1 == 1){
        let cpList = await getCPListWithCommission({body: {}})
        cpList = cpList.message || []
        // console.log("cplistttttttttttttttttttt", cpList);
        for (let index = 0; index < cpList.length; index++) {
          const element = cpList[index];
          // Check if already exist
          let query = `SELECT * FROM tbl_cp_invoice_billing WHERE billDate = '${todayDateObj.clone().format("YYYY-MM-DD")}' 
          AND JSON_UNQUOTE(JSON_EXTRACT(details, '$.cpId')) = '${element.id}' `
          let dbRes = await call({ query }, 'makeQuery', 'get');
          if(!dbRes.message.length){
            // Code to raise invoice
            let commissionFrom = todayDateObj.clone().format("YYYY-MM-DD")
            let commissionTo = todayDateObj.clone().endOf("month").format("YYYY-MM-DD")
            let apiResp = await getCPInvoiceDetails({body: {commissionFrom, commissionTo,"generateInvoice":true,"generateInvoiceForLabel":[element.name],"generateInvoiceForId":[element.id]}})
            // console.log("apiRessssssssssssssssssssssssss", apiResp);
            let summaryOfInvoiceCommission = generateInvoiceSummaryForCP(apiResp?.message, {commissionFrom, commissionTo, selectedCPDetails: element})
            // console.log("summaryOfInvoiceCommissionnnnnnnnnnn", summaryOfInvoiceCommission);
            let invoiceGenerationResp = await this.generateCPInvoiceForBilling({body: {invoices: Object.values(summaryOfInvoiceCommission)}})
            // console.log("generateInvoiceForBillinggggggggggggggg", invoiceGenerationResp);
          }
          else{
            console.log("Invoice already raised for the channel partner ", element.name, " for ", todayDateObj);
          }
        }
      }
    } catch (error) {
      console.log("error in generateInvoiceBillsForCP", error);
    }
    await sleep(21600000) //Repeat after 6 HRS
  }
}
// Cronjob to create channel partner bills for the invoice which are going to due in current month end

exports.getAllFinanciersList = async (req, res, next) => {
  try {
    let reqBody = req.body
    let temp = await emailEnabledBanks()
    let secoundlist = await enabledFinanciersForLC()
    let combinedList = temp.concat(secoundlist);
    let uniqueList = Array.from(new Map(combinedList.map(item => [item.id, item])).values());

    res.send({
      success: true,
      message: uniqueList
    })
  }
  catch (error) {
    console.log("in getAllFinanciersList error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.changeCommissionStatus = async (req, res, next) => {
  try {
    let reqBody = req.body
    let subQuery = ""
    if(reqBody.markAs){
      subQuery = ` commissionStatus = ${reqBody.markAs}, commissionDate = '${getCurrentTimeStamp()}' `
    }
    else{
      subQuery = ` commissionStatus = ${reqBody.markAs}, commissionDate = NULL `
    }
    if(reqBody.invApplicationId){
      await dbPool.query(`UPDATE tbl_invoice_discounting SET ${subQuery} WHERE id = ${reqBody.invApplicationId} `)
    }
    if(reqBody.lcApplicationId){
      await dbPool.query(`UPDATE tbl_buyer_required_lc_limit SET ${subQuery} WHERE id = ${reqBody.lcApplicationId} `)
    }
    res.send({
      success: true
    })
  }
  catch (error) {
    console.log("in changeCommissionStatus error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getAdminPaymentsResport = async (req, res, next) => {
  try {
    let reqBody = req.body
    let todayDateObj = moment()
    let query = ""
    let dbRes = null
    let response = {
      //1 
      "piedata": [
        { type: "LC", value: 0 },
        { type: "Invoice", value: 0 }
      ],
      "totalEarningLCInvoicePlan": 0,
      //2
      "planEarningBarChartData": [],
      //3 
      "commissionFromFinancierGraphData": [],
      "commissionFromFinancierDbData": [],
      //4
      "invoiceCommissionFunnelGraphData": [],
      "lcCommissionFunnelGraphData": [],
      "totalCommissionThroughPlan": 0,
      //5
      "coinsSummaryGraphData": [
        { index: 0, label: "Buyer Health", exporterCoinsUsed: 0, importerCoinsUsed: 0, financierCoinsUsed: 0 },
        { index: 1, label: 'Credit Report', exporterCoinsUsed: 0, importerCoinsUsed: 0, financierCoinsUsed: 0 },
        { index: 2, label: 'Finance Details', exporterCoinsUsed: 0, importerCoinsUsed: 0, financierCoinsUsed: 0 },
        { index: 3, label: 'Buyer/Seller Discovery', exporterCoinsUsed: 0, importerCoinsUsed: 0, financierCoinsUsed: 0 },
        { index: 4, label: 'Other Supplier', exporterCoinsUsed: 0, importerCoinsUsed: 0, financierCoinsUsed: 0 }
      ],
      "totalCoinsSummary": 0
    }

    // Earning Through Plans
    query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
    AND serviceName LIKE '%lc%' `
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["piedata"][0]["value"] = dbRes.message?.[0]?.amount || 0
    response["totalEarningLCInvoicePlan"] += (dbRes.message?.[0]?.amount || 0)

    query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
    AND serviceName LIKE '%invoice%' `
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["piedata"][1]["value"] = dbRes.message?.[0]?.amount || 0
    response["totalEarningLCInvoicePlan"] += (dbRes.message?.[0]?.amount || 0)

    // Payment Summary bar graph
    if (reqBody.paymentSummaryDuration) {
      let countForMonths = reqBody.paymentSummaryDuration?.split(" ")[0] / 1
      for (let index = 0; index < countForMonths; index++) {
        let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
        let tempToDateObj = todayDateObj.clone().subtract(index, "months")
        // For Invoice & LC Combine
        query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
        AND createdAt < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
        createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}'`
        dbRes = await call({ query }, 'makeQuery', 'get');
        response["planEarningBarChartData"].push({ label: tempToDateObj.clone().format("MMM YYYY"), value: (dbRes?.message?.[0]?.["amount"] || 0) })
      }
    }

    //Commission From Financier
    if (reqBody.commissionFrom && reqBody.commissionTo) {
      let xAxisLabel = {}
      let countForWeeksMonthsYears = moment(reqBody.commissionTo, 'YYYY-MM-DD').diff(moment(reqBody.commissionFrom, "YYYY-MM-DD"), "weeks") || 1
      xAxisLabel["name"] = "Week"
      xAxisLabel["alt"] = "weeks"
      // If more than 5 weeks convert to months
      if (countForWeeksMonthsYears / 1 > 5) {
        countForWeeksMonthsYears = moment(reqBody.commissionTo, 'YYYY-MM-DD').diff(moment(reqBody.commissionFrom, "YYYY-MM-DD"), "months") || 1
        xAxisLabel["name"] = "Month"
        xAxisLabel["alt"] = "months"
        // If more than 12 months convert to years
        if (countForWeeksMonthsYears / 1 > 12) {
          countForWeeksMonthsYears = moment(reqBody.commissionTo, 'YYYY-MM-DD').diff(moment(reqBody.commissionFrom, "YYYY-MM-DD"), "years") || 1
          xAxisLabel["name"] = "Year"
          xAxisLabel["alt"] = "years"
        }
      }
      // If countForWeeksMonthsYears less than 16 years
      if (countForWeeksMonthsYears / 1 < 16) {
        // console.log("countForWeeksMonthsYears==============>", countForWeeksMonthsYears);
        let tempArrayToStoreCharges = []
        for (let index = 0; index < countForWeeksMonthsYears; index++) {

          let tempFromDateObj = moment(reqBody.commissionFrom, "YYYY-MM-DD").add(index, xAxisLabel["alt"])
          let tempToDateObj = (index + 1) === countForWeeksMonthsYears ? moment(reqBody.commissionTo, "YYYY-MM-DD") : moment(reqBody.commissionFrom, "YYYY-MM-DD").add(index + 1, xAxisLabel["alt"])


          let tempObj = {
            index, label: `${xAxisLabel["name"]} ${index + 1}`, paid: 0, invPaid: 0, lcPaid: 0, due: 0, invDue: 0, lcDue: 0,
            paidApp: 0, invPaidApp: 0, lcPaidApp: 0, dueApp: 0, invDueApp: 0, lcDueApp: 0,
            from: tempFromDateObj.clone().format("YYYY-MM-DD"), to: tempToDateObj.clone().format("YYYY-MM-DD")
          }

          // console.log("from date, to date", tempFromDateObj, tempToDateObj);

          let extraSearchQry = ""

          if(reqBody.generateInvoiceForId?.length){
            extraSearchQry = ` AND lenderDetails.tbl_user_id IN (${reqBody.generateInvoiceForId.join(",")})`
          }

          // For Invoice
          query = `SELECT 
          tbl_invoice_discounting.created_at AS applicationCreatedAt,
          tbl_invoice_discounting.id AS applicationId, tbl_invoice_discounting.charges AS invCharges, tbl_invoice_discounting.commissionStatus AS invoiceCommissionStatus,
          tbl_invoice_discounting.commissionDate,
          lenderDetails.tbl_user_id AS lenderId, lenderDetails.company_name AS lenderName, lenderDetails.user_address AS lenderAddress, 
          sellerDetails.company_name AS sellerName, sellerDetails.user_address AS sellerAddress, 
          tbl_invoice_discounting.due_date AS invoiceDueDate , tbl_invoice_discounting.contractAmountInUSD AS contractAmount,
          tbl_mst_currency.code AS contractAmountCurrency, tbl_invoice_discounting.reference_no AS applicationNo

          FROM tbl_invoice_discounting 

          LEFT JOIN tbl_user_details lenderDetails ON
          tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id

          LEFT JOIN tbl_user_details sellerDetails ON
          tbl_invoice_discounting.seller_id = sellerDetails.tbl_user_id

          LEFT JOIN tbl_mst_currency ON
          tbl_invoice_discounting.currency = tbl_mst_currency.id
          
          WHERE tbl_invoice_discounting.charges IS NOT NULL AND tbl_invoice_discounting.due_date >= '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_invoice_discounting.due_date < '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND (tbl_invoice_discounting.status IN (3, 4, 6)) ${extraSearchQry} `
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
          // console.log("tempArrayToStoreCharges", query);

          // For LC
          // query = `SELECT 
          // tbl_buyer_required_lc_limit.createdAt AS applicationCreatedAt, tbl_buyer_required_lc_limit.id AS applicationId, tbl_buyer_required_lc_limit.charges AS lcCharges, tbl_buyer_required_lc_limit.commissionStatus AS lcCommissionStatus,
          // tbl_buyer_required_lc_limit.commissionDate,
          // lenderDetails.tbl_user_id AS lenderId, lenderDetails.company_name AS lenderName, lenderDetails.user_address AS lenderAddress,
          // sellerDetails.company_name AS sellerName, sellerDetails.user_address AS sellerAddress, 
          // tbl_buyer_required_lc_limit.invoiceDueDate , tbl_buyer_required_lc_limit.contractAmount,
          // tbl_buyer_required_lc_limit.contractAmountCurrency, tbl_buyer_required_lc_limit.invRefNo AS applicationNo
          
          // FROM tbl_buyer_required_lc_limit 

          // LEFT JOIN tbl_user_details lenderDetails ON
          // tbl_buyer_required_lc_limit.selectedFinancier = lenderDetails.tbl_user_id

          // LEFT JOIN tbl_user_details sellerDetails ON
          // tbl_buyer_required_lc_limit.createdBy = sellerDetails.tbl_user_id
          
          // WHERE tbl_buyer_required_lc_limit.charges IS NOT NULL AND tbl_buyer_required_lc_limit.invoiceDueDate BETWEEN '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          // '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND tbl_buyer_required_lc_limit.financeStatus IN (1, 3, 4) ${extraSearchQry} `

          // dbRes = await call({ query }, 'makeQuery', 'get');
          // tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)

          for (let j = 0; j < tempArrayToStoreCharges.length; j++) {
            const {lenderName, invCharges, invoiceCommissionStatus, lcCharges, 
              lcCommissionStatus,applicationNo, contractAmount, contractAmountCurrency,invoiceDueDate } = tempArrayToStoreCharges[j];

              response["commissionFromFinancierDbData"].push(tempArrayToStoreCharges[j])

            if (invCharges && invoiceCommissionStatus / 1 == 1) {
              tempObj["paidApp"] += 1
              tempObj["invPaidApp"] += 1
              tempObj["paid"] += (invCharges["totalCharges"] / invCharges["commissionPercentage"])
              tempObj["invPaid"] += (invCharges["totalCharges"] / invCharges["commissionPercentage"])
            }
            else if (invCharges && !invoiceCommissionStatus) {
              tempObj["dueApp"] += 1
              tempObj["invDueApp"] += 1
              tempObj["due"] += (invCharges["totalCharges"] / invCharges["commissionPercentage"])
              tempObj["invDue"] += (invCharges["totalCharges"] / invCharges["commissionPercentage"])
            }
            else if (lcCharges && lcCommissionStatus / 1 == 1) {
              tempObj["paidApp"] += 1
              tempObj["lcPaidApp"] += 1
              tempObj["paid"] += (lcCharges["totalCharges"] / lcCharges["commissionPercentage"])
              tempObj["lcPaid"] += (lcCharges["totalCharges"] / lcCharges["commissionPercentage"])
            }
            else if (lcCharges && !lcCommissionStatus) {
              tempObj["dueApp"] += 1
              tempObj["lcDueApp"] += 1
              tempObj["due"] += (lcCharges["totalCharges"] / lcCharges["commissionPercentage"])
              tempObj["lcDue"] += (lcCharges["totalCharges"] / lcCharges["commissionPercentage"])
            }
          }
          response["commissionFromFinancierGraphData"].push(tempObj)
          tempArrayToStoreCharges = []
        }

      }


    }

    // Earning through plans
    if(reqBody.earningFrom && reqBody.earningTo){
      // For Invoice
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Starter - Invoice' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["invoiceCommissionFunnelGraphData"].push({name: "Starter", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Growth - Invoice' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["invoiceCommissionFunnelGraphData"].push({name: "Growth", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Pro - Invoice' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["invoiceCommissionFunnelGraphData"].push({name: "Pro", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Pro Plus - Invoice' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["invoiceCommissionFunnelGraphData"].push({name: "Pro Plus", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Top-Up - Invoice' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["invoiceCommissionFunnelGraphData"].push({name: "Top-Up", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)

      // For LC
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Starter - LC' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["lcCommissionFunnelGraphData"].push({name: "Starter", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Growth - LC' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["lcCommissionFunnelGraphData"].push({name: "Growth", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Pro - LC' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["lcCommissionFunnelGraphData"].push({name: "Pro", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Pro Plus - LC' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["lcCommissionFunnelGraphData"].push({name: "Pro Plus", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(id) as noOfPurchase FROM tbl_subscription_deductions WHERE status = 2 AND type = 'CREDIT' AND (modeOfPayment != 'Plan' AND modeOfPayment != 'Coins' AND modeOfPayment != 'FREE') 
      AND serviceName = 'Top-Up - LC' AND createdAt BETWEEN '${reqBody.earningFrom}' AND '${reqBody.earningTo}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["lcCommissionFunnelGraphData"].push({name: "Top-Up", value: dbRes.message[0]["amount"] || 0}) 
      response["totalCommissionThroughPlan"] += (dbRes.message[0]["amount"] || 0)
      
    }

    // Earning through plans
    if(reqBody.coinsSummaryFrom && reqBody.coinsSummaryTo){
      // For Exporter

      // For Finance details
      query = `SELECT SUM(CAST(SUBSTRING(tbl_subscription_deductions.charges, 2) AS FLOAT)) as amount ,COUNT(tbl_subscription_deductions.id) as noOfPurchase 
      FROM tbl_subscription_deductions
      
      LEFT JOIN tbl_user_details ON
      tbl_subscription_deductions.createdBy = tbl_user_details.tbl_user_id

      LEFT JOIN tbl_user ON
      tbl_user_details.tbl_user_id = tbl_user.id

      WHERE tbl_subscription_deductions.status = 2 AND tbl_subscription_deductions.type = 'DEBIT' AND (tbl_subscription_deductions.modeOfPayment = 'Coins') 
      AND tbl_subscription_deductions.serviceName IN ('Finance Report - Last 5 years', 'Finance Report - Last 3 years', 'Finance Report - Last 2 years') 
      AND tbl_subscription_deductions.createdAt BETWEEN '${reqBody.coinsSummaryFrom}' AND '${reqBody.coinsSummaryTo}' 
      AND tbl_user.type_id = 19 `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["coinsSummaryGraphData"][2]["exporterCoinsUsed"] = dbRes.message[0]["amount"] || 0
      response["totalCoinsSummary"] += dbRes.message[0]["amount"] || 0 

      // For Buyer/Seller discovery
      query = `SELECT SUM(CAST(SUBSTRING(tbl_subscription_deductions.charges, 2) AS FLOAT)) as amount ,COUNT(tbl_subscription_deductions.id) as noOfPurchase 
      FROM tbl_subscription_deductions
      
      LEFT JOIN tbl_user_details ON
      tbl_subscription_deductions.createdBy = tbl_user_details.tbl_user_id

      LEFT JOIN tbl_user ON
      tbl_user_details.tbl_user_id = tbl_user.id

      WHERE tbl_subscription_deductions.status = 2 AND tbl_subscription_deductions.type = 'DEBIT' AND (tbl_subscription_deductions.modeOfPayment = 'Coins') 
      AND tbl_subscription_deductions.serviceName IN ('Buyer Details') 
      AND tbl_subscription_deductions.createdAt BETWEEN '${reqBody.coinsSummaryFrom}' AND '${reqBody.coinsSummaryTo}' 
      AND tbl_user.type_id = 19 `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["coinsSummaryGraphData"][3]["exporterCoinsUsed"] = dbRes.message[0]["amount"] || 0
      response["totalCoinsSummary"] += dbRes.message[0]["amount"] || 0 

      // For Other Supplier discovery
      query = `SELECT SUM(CAST(SUBSTRING(tbl_subscription_deductions.charges, 2) AS FLOAT)) as amount ,COUNT(tbl_subscription_deductions.id) as noOfPurchase 
      FROM tbl_subscription_deductions
      
      LEFT JOIN tbl_user_details ON
      tbl_subscription_deductions.createdBy = tbl_user_details.tbl_user_id

      LEFT JOIN tbl_user ON
      tbl_user_details.tbl_user_id = tbl_user.id

      WHERE tbl_subscription_deductions.status = 2 AND tbl_subscription_deductions.type = 'DEBIT' AND (tbl_subscription_deductions.modeOfPayment = 'Coins') 
      AND tbl_subscription_deductions.serviceName IN ('Top 5 Supplier Details') 
      AND tbl_subscription_deductions.createdAt BETWEEN '${reqBody.coinsSummaryFrom}' AND '${reqBody.coinsSummaryTo}' 
      AND tbl_user.type_id = 19 `
      dbRes = await call({ query }, 'makeQuery', 'get');
      response["coinsSummaryGraphData"][4]["exporterCoinsUsed"] = dbRes.message[0]["amount"] || 0
      response["totalCoinsSummary"] += dbRes.message[0]["amount"] || 0 
    }
    if(res?.send){
      res.send({
        success: true,
        message: response
      })
    }
    else{
      return({
        success: true,
        message: response
      })
    }
  }
  catch (error) {
    console.log("in getAdminPaymentsResport error--->>", error)
    if(res?.send){
      res.send({
        success: false,
        message: error
      })
    }
    else{
      return({
        success: false,
        message: error
      })
    }
  }
}

exports.generateCPInvoiceForBilling = async (req, res, next) => {
  try {
    let reqBody = req.body
    for (let index = 0; index < reqBody.invoices.length; index++) {
      const element = reqBody.invoices[index];
      if (element?.billNo) {
        // Decide invoice status based on transaction status start
        let isWholeInvoicePaid = true
        for (let j = 0; j < element.data.length; j++) {
          const k = element.data[j];
          if (k[8] != "Paid") {
            isWholeInvoicePaid = false
          }
        }
        // Decide invoice status based on transaction status end
        let query = formatSqlQuery(`INSERT INTO tbl_cp_invoice_billing (billNo, billDate, details, status) VALUES (?, ?, ?, ?) `, [element.billNo, element.billCreatedAt, jsonStr(element), isWholeInvoicePaid ? 1 : 0])
        await dbPool.query(query)
      }
    }
    if(res?.send){
      res.send({
        success: true
      })
    }
    else{
      return({
        success: true
      })
    }
  }
  catch (error) {
    console.log("in generateCPInvoiceForBilling error--->>", error)
    if(res?.send){
      res.send({
        success: false,
        message: error
      })
    }
    else{
      return({
        success: false,
        message: error
      })
    }
  }
}

const generateBillNo = async (billDate) => {
  let query = `SELECT id FROM tbl_invoice_billing `
  let dbRes = await call({ query }, 'makeQuery', 'get');
  let thisYear = moment(billDate).format("YY")/1
  return `${dbRes.message.length + 1}/${thisYear}-${thisYear+1}`
}


exports.uploadBilledInvoice = async (req, res, next) => {
  try {
    let reqBody = req.body
    let reqFiles = req.files
    let billNo = await generateBillNo(reqBody.billCreatedAt)
    fs.writeFileSync('./docs/' + reqFiles["file"].md5, reqFiles["file"].data);
    let dbRes = await dbPool.query(`INSERT INTO tbl_document_details (doc_name, file_name, gen_doc_label, file_hash,
          created_at, created_by, modified_at ) VALUE ("Invoice Attachment", "${reqFiles["file"].name}", "Invoice Attachment",
          "${reqFiles["file"].md5}", "${getCurrentTimeStamp()}", "${reqBody.userId}", "${getCurrentTimeStamp()}") `)
    let detailsObj = {
      "data": [],
      "item": {
          "lenderId": reqBody.lenderId,
          "invCharges": {},
          "lenderName": reqBody.lenderName,
          "invoiceCommissionStatus": 1
      },
      billNo,
      "commissionTo": reqBody.commissionTo,
      "billCreatedAt": reqBody.billCreatedAt,
      "commissionFrom": reqBody.commissionFrom,
      "commissionPayout": reqBody.billAmountInUsd
    }
    let query = formatSqlQuery(`INSERT INTO tbl_invoice_billing (billNo, billDate, attachment, status, details) VALUES (?, ?, ?, ?, ?) `,
      [billNo, reqBody.billCreatedAt, dbRes[0]["insertId"], 1, jsonStr(detailsObj)])
    await dbPool.query(query)
    res.send({
      success: true,
      message: "Invoice uploaded."
    })
  }
  catch (error) {
    // console.log("error in uploadBilledInvoice", error)
    res.send({
      success: false,
      message: error
    })
  }
}


exports.generateInvoiceForBilling = async (req, res, next) => {
  try {
    let reqBody = req.body
    let reqFiles = req.files
    reqBody["invoices"] = JSON.parse(reqBody.invoices)
    console.log("rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr", reqBody.invoices);
    for (let index = 0; index < reqBody.invoices.length; index++) {
      const element = reqBody.invoices[index];
      let billNo = await generateBillNo(element.billCreatedAt)
      if (element.billCreatedAt && billNo) {
        element["billNo"] = billNo
        // Decide invoice status based on transaction status start
        let isWholeInvoicePaid = true
        for (let j = 0; j < element.data?.length; j++) {
          const k = element.data[j];
          if (k[8] != "Paid") {
            isWholeInvoicePaid = false
          }
        }
        // Decide invoice status based on transaction status end
        let query = formatSqlQuery(`INSERT INTO tbl_invoice_billing (billNo, billDate, details, status) VALUES (?, ?, ?, ?) `,
        [billNo, element.billCreatedAt, jsonStr(element), isWholeInvoicePaid ? 1 : 0])
        await dbPool.query(query)
        // update attachment if present
        if(reqFiles?.file){
          fs.writeFileSync('./docs/' + reqFiles["file"].md5, reqFiles["file"].data);
          let docDbResp = await dbPool.query(`INSERT INTO tbl_document_details (doc_name, file_name, gen_doc_label, file_hash,
            created_at, created_by, modified_at ) VALUE ("Invoice Attachment", "${reqFiles["file"].name}", "Invoice Attachment",
            "${reqFiles["file"].md5}", "${getCurrentTimeStamp()}", "${reqBody.userId}", "${getCurrentTimeStamp()}") `)
          await dbPool.query(formatSqlQuery('UPDATE tbl_invoice_billing SET attachment = ? WHERE billNo = ? ',[docDbResp[0]["insertId"],billNo]))
        }
      }
    }
    if(res?.send){
      res.send({
        success: true
      })
    }
    else{
      return({
        success: true
      })
    }
  }
  catch (error) {
    console.log("in generateInvoiceForBilling error--->>", error)
    if(res?.send){
      res.send({
        success: false,
        message: error
      })
    }
    else{
      return({
        success: false,
        message: error
      })
    }
  }
}


exports.getGenerateInvoiceBills = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filter = {}
    let paginationQry = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage} `
    let searchQuery = ""
    if(reqBody.search){
      searchQuery = ` AND tbl_invoice_billing.billNo LIKE '%${reqBody.search}%' `
    }
    let query = `SELECT tbl_invoice_billing.* FROM tbl_invoice_billing
    WHERE 1 ${searchQuery}
    ORDER BY tbl_invoice_billing.billDate DESC
    ${paginationQry} `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    let countQuery = `SELECT id FROM tbl_invoice_billing WHERE 1 ${searchQuery} `
    let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');
    // Due count
    let filterQuery = `SELECT COUNT(id) AS totalApplicationCount, 
    SUM(details->'$.commissionPayout') AS totalInvoiceAmount FROM tbl_invoice_billing WHERE status = 0 AND
    billDate > '${moment().subtract(1, "month").format("YYYY-MM-DD")}' `
    let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    filter["due"] = filterDbRes.message[0]
    // Due count
    filterQuery = `SELECT COUNT(id) AS totalApplicationCount, 
    SUM(details->'$.commissionPayout') AS totalInvoiceAmount FROM tbl_invoice_billing WHERE status = 0 AND
    billDate < '${moment().subtract(1, "month").format("YYYY-MM-DD")}' `
    filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    filter["overdue"] = filterDbRes.message[0]
    // Received count
    filterQuery = `SELECT COUNT(id) AS totalApplicationCount, 
    SUM(details->'$.commissionPayout') AS totalInvoiceAmount FROM tbl_invoice_billing WHERE status = 1 `
    filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    filter["received"] = filterDbRes.message[0]
    // Receivables from next month onwards count
    filterQuery = `SELECT charges FROM tbl_invoice_discounting 
    WHERE due_date >= '${moment().add(1, "month").format("YYYY-MM-01")}' AND commissionDate IS NULL AND charges IS NOT NULL `
    filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    filter["receivables"] = {"totalInvoiceAmount" : 0}
    for (let index = 0; index < filterDbRes.message.length; index++) {
      const element = filterDbRes.message[index];
      let charges = element["charges"]
      filter["receivables"]["totalInvoiceAmount"] += (charges["totalCharges"] * (charges["commissionPercentage"]/100)) 
    }
    res.send({
      success: true,
      message: {
        data: dbRes.message,
        count: countDbRes.message.length,
        filter
      }
    })
  }
  catch (error) {
    console.log("in getGenerateInvoiceBills error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getCPGenerateInvoiceBills = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filter = {}
    let paginationQry = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage} `
    let searchQuery = ""
    if(reqBody.onlyShowForUserId){
      searchQuery += ` AND tbl_cp_invoice_billing.details->'$.cpId' = ${reqBody.onlyShowForUserId} `
    }
    if(reqBody.search){
      searchQuery += ` AND tbl_cp_invoice_billing.billNo LIKE '%${reqBody.search}%' `
    }
    let query = `SELECT tbl_cp_invoice_billing.* FROM tbl_cp_invoice_billing
    WHERE 1 ${searchQuery}
    ORDER BY tbl_cp_invoice_billing.id DESC
    ${paginationQry} `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    let countQuery = `SELECT id FROM tbl_cp_invoice_billing WHERE 1 ${searchQuery} `
    let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');
    // Due count
    let filterQuery = `SELECT COUNT(id) AS totalApplicationCount, 
    SUM(details->'$.totalCPCommissionAmount') AS totalInvoiceAmount FROM tbl_cp_invoice_billing WHERE status = 0 AND
    billDate > '${moment().subtract(1, "month").format("YYYY-MM-DD")}' `
    let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    filter["due"] = filterDbRes.message[0]
    // Due count
    filterQuery = `SELECT COUNT(id) AS totalApplicationCount, 
    SUM(details->'$.totalCPCommissionAmount') AS totalInvoiceAmount FROM tbl_cp_invoice_billing WHERE status = 0 AND
    billDate < '${moment().subtract(1, "month").format("YYYY-MM-DD")}' `
    filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    filter["overdue"] = filterDbRes.message[0]
    // paid count
    filterQuery = `SELECT COUNT(id) AS totalApplicationCount, 
    SUM(details->'$.totalCPCommissionAmount') AS totalInvoiceAmount FROM tbl_cp_invoice_billing WHERE status = 1 `
    filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    filter["paid"] = filterDbRes.message[0]
    // Payables from next month onwards count not working
    // filterQuery = `SELECT charges FROM tbl_invoice_discounting 
    // WHERE due_date >= '${moment().add(1, "month").format("YYYY-MM-01")}' AND commissionDate IS NULL AND charges IS NOT NULL `
    // filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    // filter["payables"] = {"totalInvoiceAmount" : 0}
    // for (let index = 0; index < filterDbRes.message.length; index++) {
    //   const element = filterDbRes.message[index];
    //   let charges = element["charges"]
    //   filter["payables"]["totalInvoiceAmount"] += (charges["totalCharges"] / charges["commissionPercentage"]) 
    // }
    res.send({
      success: true,
      message: {
        data: dbRes.message,
        count: countDbRes.message.length,
        filter
      }
    })
  }
  catch (error) {
    console.log("in getCPGenerateInvoiceBills error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.changeInvoiceBillStatus = async (req, res, next) => {
  try {
    let reqBody = req.body
    let transactionDetails = []
    let query = `SELECT * FROM tbl_invoice_billing WHERE billNo = '${reqBody.billNo}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    transactionDetails = dbRes.message[0]["details"]
    console.log("transactionDetails",transactionDetails.data)
    for (let index = 0; index < transactionDetails["data"].length; index++) {
      transactionDetails["data"][index][8] = reqBody.markAs/1 == 1 ? "Paid" : "Due"
      transactionDetails["data"][index][9] = reqBody.markAs/1 == 1 ? moment().format("YYYY-MM-DD") : null
      console.log("transactionDetails[index][9]",transactionDetails["data"][index][9])
      if(!transactionDetails["data"][index][9]){
        console.log("came inside else",reqBody.markAs)
        await dbPool.query(`UPDATE tbl_invoice_discounting SET commissionStatus = '${reqBody.markAs}', commissionDate = null 
        WHERE reference_no = '${transactionDetails["data"][index][2].split("Invoice Discounting -")[1].trim()}' `)
        await dbPool.query(formatSqlQuery(`UPDATE tbl_invoice_billing SET status = ?, details = ? WHERE billNo = ? `,
          [reqBody.markAs, jsonStr(transactionDetails), reqBody.billNo]))
      }
      else{
        await dbPool.query(`UPDATE tbl_invoice_discounting SET commissionStatus = '${reqBody.markAs}', commissionDate = '${transactionDetails["data"][index][9]}' 
        WHERE reference_no = '${transactionDetails["data"][index][2].split("Invoice Discounting -")[1].trim()}' `)}
        await dbPool.query(formatSqlQuery(`UPDATE tbl_invoice_billing SET status = ?, details = ? WHERE billNo = ? `,
        [reqBody.markAs, jsonStr(transactionDetails), reqBody.billNo]))
    }
    res.send({
      success: true,
      message: "Invoice & transaction status changed successfully"
    })
  }
  catch (error) {
    console.log("in changeInvoiceBillStatus error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.deleteInvoiceBill = async (req, res, next) => {
  try {
    let reqBody = req.body
    await dbPool.query(`DELETE FROM tbl_invoice_billing WHERE billNo = '${reqBody.billNo}' `)
    res.send({
      success: true,
      message: "Invoice bill removed"
    })
  }
  catch (error) {
    console.log("in deleteInvoiceBill error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}


exports.deleteCPInvoiceBill = async (req, res, next) => {
  try {
    let reqBody = req.body
    await dbPool.query(`DELETE FROM tbl_cp_invoice_billing WHERE billNo = '${reqBody.billNo}' `)
    res.send({
      success: true,
      message: "Invoice bill removed"
    })
  }
  catch (error) {
    console.log("in deleteCPInvoiceBill error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.changeCPInvoiceBillStatus = async (req, res, next) => {
  try {
    let reqBody = req.body
    let query = `SELECT * FROM tbl_cp_invoice_billing WHERE billNo = '${reqBody.billNo}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    await dbPool.query(`UPDATE tbl_cp_invoice_billing SET status = '${reqBody.markAs}' WHERE billNo = '${reqBody.billNo}' `)
    res.send({
      success: true,
      message: "Invoice status changed successfully"
    })
  }
  catch (error) {
    console.log("in changeCPInvoiceBillStatus error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

