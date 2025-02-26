const { dbPool } = require("../../src/database/mysql")
const { call } = require("../../utils/codeHelper")
const moment = require("moment");
const fs = require('fs');
const e = require("cors");
const { nullCheck } = require("../../database/utils/utilFuncs");
const { count } = require("console");
const { sendMail } = require("../../utils/mailer");
const config = require("../../config");
const { emailEnabledBanks, enabledFinanciersForLC, activeUserLogicDurationInWeeks } = require("../../urlCostants");
const { getCurrentTimeStamp, getInvoiceFinalChargesObj, convertValueIntoUSD, formatSqlQuery, jsonStr } = require("../../iris_server/utils");
const { getModifiApiToken, getDealInfo } = require("../../src/cronjobs/modifi");

exports.pullInvoiceChargesFromFinancier = async () => {
  try {
    // For modifi
    let qry = `SELECT finId, invCommissionPercentage FROM tbl_financier_metadata`
    let dbRsp = await call({ query: qry }, 'makeQuery', 'get');
    let allFinancierCharges = dbRsp.message
    let modifiCommissionPercentage = allFinancierCharges.filter((i, j) => {
      if(i.finId/1 == 748){
        return true
      }
    })?.[0]?.["invCommissionPercentage"] || 15
    let query = `SELECT * FROM tbl_invoice_discounting WHERE modifi_deal_id IS NOT NULL AND charges IS NULL`
    let dbRes = await call({ query }, 'makeQuery', 'get');
    // console.log("pullInvoiceChargesFromFinancier", dbRes.message);
    let modifiApiToken = await getModifiApiToken()
    // console.log("modifiApiToken========================>", modifiApiToken);
    if (dbRes.message.length && modifiApiToken) {
      for (let index = 0; index < dbRes.message.length; index++) {
        const element = dbRes.message[index];
        let modifiDealResp = await getDealInfo(modifiApiToken, element)
        // console.log("modifiDealResp==============================>", modifiDealResp); 
        if (Object.keys(modifiDealResp?.fees || {}).length) {
          let totalChargesWithCurrency = 0
          let totalChargesCurrency = "NA"
          for (let j = 0; j < Object.keys(modifiDealResp?.fees).length; j++) {
            const item = Object.keys(modifiDealResp?.fees)[j];
            modifiDealResp.fees[item]["amount"] = modifiDealResp.fees[item]["amount"] / 100
            totalChargesWithCurrency += modifiDealResp.fees[item]["amount"]
            totalChargesCurrency = modifiDealResp.fees[item]["currency"] || "NA"
          }
          let chargesToInsert = {
            ...modifiDealResp.fees,
            commissionPercentage: modifiCommissionPercentage,
            totalCharges: await convertValueIntoUSD(totalChargesWithCurrency, totalChargesCurrency),
            totalChargesCurrency,
            totalChargesWithCurrency
          }
          await dbPool.query(formatSqlQuery(`UPDATE tbl_invoice_discounting SET charges = ? WHERE id = ? `, 
            [jsonStr(chargesToInsert), element.id]))
        }
      }
    }
    // For all other financiers
    query = `SELECT tbl_invoice_discounting.*,
    tbl_mst_currency.code AS invoiceCurrency,
    tbl_buyer_required_limit.buyers_credit 
    FROM tbl_invoice_discounting
    LEFT JOIN tbl_buyers_detail ON 
    tbl_buyers_detail.id = tbl_invoice_discounting.buyer_id
    LEFT JOIN tbl_buyer_required_limit ON
    tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId
    LEFT JOIN tbl_mst_currency ON
    tbl_invoice_discounting.currency = tbl_mst_currency.id
    WHERE tbl_invoice_discounting.modifi_deal_id IS NULL AND tbl_invoice_discounting.charges IS NULL AND 
    tbl_buyer_required_limit.buyers_credit IS NOT NULL `
    dbRes = await call({ query }, 'makeQuery', 'get');
    // console.log("1111111111111111111111111111111111111");
    if(dbRes.message.length){
      for (let index = 0; index < dbRes.message.length; index++) {
        const element = dbRes.message[index];
        let buyersCredit = element.buyers_credit ? JSON.parse(element.buyers_credit) : []
        let lenderWiseQuoteDetails = buyersCredit.filter((i) => {
          if(i.lender_id/1 == element.lender_id/1){
            return i
          }
        })?.[0] || {} 
        // console.log("2222222222222222222222222222222222222222", element, lenderWiseQuoteDetails, getInvoiceFinalChargesObj(element.contract_amount, element.credit_days, lenderWiseQuoteDetails));
        let chargesToInsert = {
          ...getInvoiceFinalChargesObj(element.contract_amount/1, element.credit_days/1, lenderWiseQuoteDetails),
          commissionPercentage: allFinancierCharges.filter((i,j) => {
            if(i.finId/1 == element.lender_id/1){
              return true
            }
          })?.[0]?.invCommissionPercentage || 10,
          totalChargesCurrency: element.invoiceCurrency || "USD"
        }
        chargesToInsert["totalCharges"] = await convertValueIntoUSD(chargesToInsert["totalChargesWithCurrency"], chargesToInsert["totalChargesCurrency"])
        // console.log("3333333333333333333333333333333333333333", chargesToInsert);
        await dbPool.query(formatSqlQuery(`UPDATE tbl_invoice_discounting SET charges = ? WHERE id = ? `, 
          [jsonStr(chargesToInsert), element.id]))
      }
    }
  } catch (error) {
    console.log("error in pullInvoiceChargesFromFinancier", error);
  }
}

exports.getAdminDashboardStats = async (req, res, next) => {
  try {
    let reqBody = req.body
    let response = {
      lifetimeEarnings: 0,
      creditIssued: 0,
      clientsOnboarded: 0,
      activeUsers: 0,
      inactiveUsers: 0
    }
    let query = ""
    let dbRes = null
    let todayDateObj = moment()
    let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
    let extraQuery = ''
    let extraQueryOnboard = ''
    if(reqBody.onlyShowForUserId){
      extraQuery = ` AND tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}'`
      extraQueryOnboard = ` AND LeadAssignedTo = '${reqBody.onlyShowForUserId}'`
    }
    if(reqBody.subadminIds){
      extraQuery += ` AND tbl_user.LeadAssignedTo IN ('${reqBody.subadminIds?.join("','")}')`
      extraQueryOnboard += ` AND LeadAssignedTo IN ('${reqBody.subadminIds?.join("','")}')`
    
    }
    // Lifetime earnings by months
    let earningGraphData = []
    if (reqBody.from && reqBody.to) {
      const todaysDate = new Date()
      const toDate = new Date(reqBody.to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(reqBody.to)
      let countForMonths =  moment(reqBody.to).diff(reqBody.from,'month') + 1
      console.log('CountFor Monthssssss',countForMonths);
      let earningTableData = []
      let plansAmt = 0
      let commissionAmt = 0
      // For Months
      if(countForMonths > 3){
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
          let tempToDateObj = todayDateObj.clone().subtract(index, "months")
          let tempTotalEarnings = 0
          let tempArrayToStoreCharges = []

          // // For Invoice
          // query = `SELECT charges FROM tbl_invoice_discounting LEFT JOIN tbl_user ON tbl_invoice_discounting.seller_id = tbl_user.id WHERE charges IS NOT NULL AND tbl_invoice_discounting.created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
          // tbl_invoice_discounting.created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' AND commissionStatus = 1 ${extraQuery} ORDER BY tbl_invoice_discounting.created_at ASC`
          // dbRes = await call({ query }, 'makeQuery', 'get');
          // tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
          // // For LC
          // query = `SELECT charges FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id WHERE charges IS NOT NULL AND tbl_buyer_required_lc_limit.updatedAt < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
          // tbl_buyer_required_lc_limit.updatedAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' AND tbl_buyer_required_lc_limit.commissionStatus = 1 ${extraQuery} ORDER BY tbl_buyer_required_lc_limit.updatedAt ASC`
          // dbRes = await call({ query }, 'makeQuery', 'get');
          // tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)

          query = `SELECT SUM(JSON_EXTRACT(details, '$.commissionPayout')) AS lifetimeEarnings
          FROM tbl_invoice_billing WHERE billDate BETWEEN '${tempToDateObj.clone().format("YYYY-MM-01")}' AND
          '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND status = 1 `
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempTotalEarnings += (dbRes.message[0]["lifetimeEarnings"] || 0)
  
          // For Invoice & LC Combine Plan Earning
          query = `SELECT SUM(CAST(SUBSTRING(charges, 2) AS FLOAT)) as amount ,COUNT(tbl_subscription_deductions.id) as noOfPurchase FROM tbl_subscription_deductions  LEFT JOIN tbl_user ON tbl_subscription_deductions.createdBy = tbl_user.id WHERE tbl_subscription_deductions.status = 2 AND tbl_subscription_deductions.type = 'CREDIT' AND (tbl_subscription_deductions.modeOfPayment != 'Plan' AND tbl_subscription_deductions.modeOfPayment != 'Coins' AND tbl_subscription_deductions.modeOfPayment != 'FREE') 
          AND tbl_subscription_deductions.createdAt < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
          tbl_subscription_deductions.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQuery} ORDER BY tbl_subscription_deductions.createdAt ASC`
          dbRes = await call({ query }, 'makeQuery', 'get');
  
          for (let j = 0; j < tempArrayToStoreCharges.length; j++) {
            const element = tempArrayToStoreCharges[j]["charges"];
            tempTotalEarnings += (element["totalCharges"] / element["commissionPercentage"])
          }
          plansAmt += (dbRes?.message?.[0]?.["amount"] || 0)
          commissionAmt += tempTotalEarnings

          earningGraphData.push({ label: tempToDateObj.clone().format("MMM YYYY"), value: tempTotalEarnings, valuePlan:(dbRes?.message?.[0]?.["amount"] || 0) })
          earningTableData.push([tempToDateObj.clone().format("MMM YYYY"), "$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(tempTotalEarnings) ,"$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(dbRes?.message?.[0]?.["amount"] || 0)] )
        }
        response["earningGraphData"] = earningGraphData.reverse()
        response["earningTableData"] = earningTableData.reverse().concat([["Total", "$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(commissionAmt) ,"$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(plansAmt) ]])
      }
      // For Days 
      else if(countForMonths <= 1){
        countForMonths = moment(todayDateObj).clone().diff(reqBody.from, "days")
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
          let tempToDateObj = todayDateObj.clone().subtract(index, "day")
          let tempTotalEarnings = 0
          let tempArrayToStoreCharges = []
          // For Invoice
          query = `SELECT tbl_invoice_discounting.charges FROM tbl_invoice_discounting LEFT JOIN tbl_user ON tbl_invoice_discounting.seller_id = tbl_user.id WHERE tbl_invoice_discounting.charges IS NOT NULL AND tbl_invoice_discounting.created_at < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_invoice_discounting.created_at >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND tbl_invoice_discounting.commissionStatus = 1 ${extraQuery} ORDER BY tbl_invoice_discounting.created_at ASC`
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
          // For LC
          query = `SELECT tbl_buyer_required_lc_limit.charges FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id WHERE tbl_buyer_required_lc_limit.charges IS NOT NULL AND tbl_buyer_required_lc_limit.updatedAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_buyer_required_lc_limit.updatedAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND tbl_buyer_required_lc_limit.commissionStatus = 1 ${extraQuery} ORDER BY tbl_buyer_required_lc_limit.updatedAt ASC`
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
  
          // For Invoice & LC Combine Plan Earning
          query = `SELECT SUM(CAST(SUBSTRING(tbl_subscription_deductions.charges, 2) AS FLOAT)) as amount ,COUNT(tbl_subscription_deductions.id) as noOfPurchase FROM tbl_subscription_deductions LEFT JOIN tbl_user ON tbl_subscription_deductions.createdBy = tbl_user.id WHERE tbl_subscription_deductions.status = 2 AND tbl_subscription_deductions.type = 'CREDIT' AND (tbl_subscription_deductions.modeOfPayment != 'Plan' AND tbl_subscription_deductions.modeOfPayment != 'Coins' AND tbl_subscription_deductions.modeOfPayment != 'FREE') 
          AND tbl_subscription_deductions.createdAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_subscription_deductions.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQuery} ORDER BY tbl_subscription_deductions.createdAt ASC`
          dbRes = await call({ query }, 'makeQuery', 'get');
  
          for (let j = 0; j < tempArrayToStoreCharges.length; j++) {
            const element = tempArrayToStoreCharges[j]["charges"];
            tempTotalEarnings += (element["totalCharges"] / element["commissionPercentage"])
          }
          plansAmt += (dbRes?.message?.[0]?.["amount"] || 0)
          commissionAmt += tempTotalEarnings
          earningGraphData.push({ label: tempToDateObj.clone().format("DD MMM"), value: tempTotalEarnings, valuePlan:(dbRes?.message?.[0]?.["amount"] || 0) })
          earningTableData.push([tempToDateObj.clone().format("DD MMM"), "$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(tempTotalEarnings) ,"$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(dbRes?.message?.[0]?.["amount"] || 0)] )
        }
        response["earningGraphData"] = earningGraphData.reverse()
        response["earningTableData"] = earningTableData.reverse().concat([["Total", "$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(commissionAmt) ,"$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(plansAmt) ]])
      }
      // For Weeks
      else {
        countForMonths = moment(todayDateObj).clone().diff(reqBody.from, "weeks")
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
          let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
          let tempTotalEarnings = 0
          let tempArrayToStoreCharges = []
          // For Invoice
          query = `SELECT tbl_invoice_discounting.charges FROM tbl_invoice_discounting LEFT JOIN tbl_user ON tbl_invoice_discounting.seller_id = tbl_user.id WHERE tbl_invoice_discounting.charges IS NOT NULL AND tbl_invoice_discounting.created_at < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_invoice_discounting.created_at >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND tbl_invoice_discounting.commissionStatus = 1 ${extraQuery} ORDER BY tbl_invoice_discounting.created_at ASC `
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
          // For LC
          query = `SELECT charges FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id  WHERE tbl_buyer_required_lc_limit.charges IS NOT NULL AND tbl_buyer_required_lc_limit.updatedAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_buyer_required_lc_limit.updatedAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND tbl_buyer_required_lc_limit.commissionStatus = 1 ${extraQuery} ORDER BY tbl_buyer_required_lc_limit.updatedAt ASC`
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
  
          // For Invoice & LC Combine Plan Earning
          query = `SELECT SUM(CAST(SUBSTRING(tbl_subscription_deductions.charges, 2) AS FLOAT)) as amount ,COUNT(tbl_subscription_deductions.id) as noOfPurchase FROM tbl_subscription_deductions  LEFT JOIN tbl_user ON tbl_subscription_deductions.createdBy = tbl_user.id WHERE tbl_subscription_deductions.status = 2 AND tbl_subscription_deductions.type = 'CREDIT' AND (tbl_subscription_deductions.modeOfPayment != 'Plan' AND tbl_subscription_deductions.modeOfPayment != 'Coins' AND tbl_subscription_deductions.modeOfPayment != 'FREE') 
          AND tbl_subscription_deductions.createdAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_subscription_deductions.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQuery} ORDER BY tbl_subscription_deductions.createdAt ASC`
          dbRes = await call({ query }, 'makeQuery', 'get');
  
          for (let j = 0; j < tempArrayToStoreCharges.length; j++) {
            const element = tempArrayToStoreCharges[j]["charges"];
            tempTotalEarnings += (element["totalCharges"] / element["commissionPercentage"])
          }
          plansAmt += (dbRes?.message?.[0]?.["amount"] || 0)
          commissionAmt += tempTotalEarnings
          
          earningGraphData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, value: tempTotalEarnings, valuePlan:(dbRes?.message?.[0]?.["amount"] || 0) })
          earningTableData.push([`${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, "$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(tempTotalEarnings) ,"$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(dbRes?.message?.[0]?.["amount"] || 0)] )
        }
        response["earningTableData"] = earningGraphData.reverse()
        response["earningTableData"] = earningTableData.reverse().concat([["Total", "$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(commissionAmt) ,"$ " + Intl.NumberFormat('en-US', { notation: 'compact' }).format(plansAmt) ]])

      }
    }

    // Customer onboarded by months
    let customerOnboardedData = []
    let customerOnboardTableData = []
    let overallExp = 0
    let overallBuyers= 0
    let overallCP=0
    if (reqBody.from && reqBody.to) {
      const todaysDate = new Date()
      const toDate = new Date(reqBody.to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(reqBody.to)
      let customerOnboardedData = []
      let countForMonths =  moment(reqBody.to).diff(reqBody.from,'month') + 1
      // For Months
      if (countForMonths > 3) {
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
          let tempToDateObj = todayDateObj.clone().subtract(index, "months")
      
          let tempCustomerOnboarded = 0
          query = `SELECT id FROM tbl_user WHERE type_id = 19 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
        created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQueryOnboard}`
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempCustomerOnboarded = dbRes.message.length

          let tempFinOnboarded = 0
          query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
          LEFT JOIN tbl_buyer_required_limit ON
          tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_limit.userId = tbl_user.id
          WHERE tbl_buyer_required_limit.id IS NOT NULL AND tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND  	tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQueryOnboard}`
          let dbResFin = await call({ query }, 'makeQuery', 'get');
          tempFinOnboarded = dbResFin.message.length

          let tempCPOnboarded = 0
          query = `SELECT id FROM tbl_user WHERE type_id = 20 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
          created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQuery}`
          let dbResCP = await call({ query }, 'makeQuery', 'get');
          tempCPOnboarded = dbResCP.message.length

          customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), value: tempCustomerOnboarded, FinValue: tempFinOnboarded, CPValue: tempCPOnboarded })
          customerOnboardTableData.push([tempToDateObj.clone().format("MMM YYYY"),tempCustomerOnboarded,tempFinOnboarded,tempCPOnboarded])
          overallExp +=tempCustomerOnboarded
          overallBuyers += tempFinOnboarded
          overallCP +=tempCPOnboarded
        }
        response["customerOnboardedData"] = customerOnboardedData.reverse()
        response["customerOnboardTableData"] = customerOnboardTableData.reverse().concat([["Total",overallExp,overallBuyers,overallCP]])

      }
      // For Days
      else if (countForMonths <= 1) {
        countForMonths = moment(todayDateObj).clone().diff(reqBody.from, "days")
       
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
          let tempToDateObj = todayDateObj.clone().subtract(index, "day")
          let tempCustomerOnboarded = 0
          query = `SELECT id FROM tbl_user WHERE type_id = 19 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          created_at >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQueryOnboard}`
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempCustomerOnboarded = dbRes.message.length

          let tempFinOnboarded = 0
          query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
          LEFT JOIN tbl_buyer_required_limit ON
          tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_limit.userId = tbl_user.id
          WHERE tbl_buyer_required_limit.id IS NOT NULL AND  	tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQuery}`
          let dbResFin = await call({ query }, 'makeQuery', 'get');
          tempFinOnboarded = dbResFin.message.length

          let tempCPOnboarded = 0
          query = `SELECT id FROM tbl_user WHERE type_id = 20 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          created_at >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQueryOnboard}`
          let dbResCP = await call({ query }, 'makeQuery', 'get');
          tempCPOnboarded = dbResCP.message.length
          overallExp +=tempCustomerOnboarded
          overallBuyers += tempFinOnboarded
          overallCP +=tempCPOnboarded

          customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"), value: tempCustomerOnboarded, FinValue: tempFinOnboarded, CPValue: tempCPOnboarded })
          customerOnboardTableData.push([tempToDateObj.clone().format("DD MMM"),tempCustomerOnboarded,tempFinOnboarded,tempCPOnboarded])

        }
        response["customerOnboardedData"] = customerOnboardedData.reverse()
        response["customerOnboardTableData"] = customerOnboardTableData.reverse().concat([["Total",overallExp,overallBuyers,overallCP]])

      }
      // For Weeks
      else {
        countForMonths = moment(todayDateObj).clone().diff(reqBody.from, "weeks")
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
          let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
          let tempCustomerOnboarded = 0
          query = `SELECT id FROM tbl_user WHERE type_id = 19 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
            created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQueryOnboard}`
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempCustomerOnboarded = dbRes.message.length
          let tempFinOnboarded = 0
          query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
          LEFT JOIN tbl_buyer_required_limit ON
          tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_limit.userId = tbl_user.id
          WHERE tbl_buyer_required_limit.id IS NOT NULL AND 	tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
          tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQuery}`
          let dbResFin = await call({ query }, 'makeQuery', 'get');
          tempFinOnboarded = dbResFin.message.length

          let tempCPOnboarded = 0
          query = `SELECT id FROM tbl_user WHERE type_id = 20 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
            created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQueryOnboard}`
          let dbResCP = await call({ query }, 'makeQuery', 'get');
          tempCPOnboarded = dbResCP.message.length
          
          overallExp +=tempCustomerOnboarded
          overallBuyers += tempFinOnboarded
          overallCP +=tempCPOnboarded

          customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, value: tempCustomerOnboarded, FinValue: tempFinOnboarded, CPValue: tempCPOnboarded })
          customerOnboardTableData.push([`${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`,tempCustomerOnboarded,tempFinOnboarded,tempCPOnboarded])

        }
        response["customerOnboardedData"] = customerOnboardedData.reverse()
        response["customerOnboardTableData"] = customerOnboardTableData.reverse().concat([["Total",overallExp,overallBuyers,overallCP]])
      }
    }

    // // Lifetime earnings
    // let tempArrayToStoreCharges = []
    // // For Invoice
    // query = `SELECT tbl_invoice_discounting.charges FROM tbl_invoice_discounting LEFT JOIN tbl_user ON tbl_invoice_discounting.seller_id = tbl_user.id WHERE tbl_invoice_discounting.charges IS NOT NULL AND tbl_invoice_discounting.commissionStatus = 1 ${extraQuery}`
    // dbRes = await call({ query }, 'makeQuery', 'get');
    // tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
    // // For LC
    // query = `SELECT tbl_buyer_required_lc_limit.charges FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id WHERE tbl_buyer_required_lc_limit.charges IS NOT NULL AND tbl_buyer_required_lc_limit.commissionStatus = 1 ${extraQuery}`
    // dbRes = await call({ query }, 'makeQuery', 'get');
    // tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
    // for (let index = 0; index < tempArrayToStoreCharges.length; index++) {
    //   const element = tempArrayToStoreCharges[index]["charges"];
    //   response["lifetimeEarnings"] += (element["totalCharges"] / element["commissionPercentage"])
    // }

    // Lifetime earnings New Version 
    query = `SELECT SUM(JSON_EXTRACT(details, '$.commissionPayout')) AS lifetimeEarnings
    FROM tbl_invoice_billing WHERE status = 1 `
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["lifetimeEarnings"] = (dbRes.message[0]["lifetimeEarnings"] || 0)

    // Credit Issued
    // query = `SELECT * FROM tbl_buyer_required_limit`
    // dbRes = await call({ query }, 'makeQuery', 'get');
    // if (dbRes.message.length) {
    //   for (let index = 0; index < dbRes.message.length; index++) {
    //     const element = dbRes.message[index];
    //     if (element.buyers_credit) {
    //       let buyers_credit = JSON.parse(element.buyers_credit)
    //       for (let j = 0; j < buyers_credit.length; j++) {
    //         const item = buyers_credit[j];
    //         if (item.financierAction != "deny" && item.financeLimit && !isNaN(item.financeLimit)) {
    //           response["creditIssued"] += item.financeLimit / 1
    //         }
    //       }
    //     }
    //   }
    // }

    // Total discounting
    let dateRangeQueryForInvoice = ` AND tbl_buyer_required_limit.updatedAt >= '${reqBody.from}' AND tbl_buyer_required_limit.updatedAt <= '${reqBody.to}'  `
    let dateRangeQueryForLC = ` AND tbl_buyer_required_lc_limit.updatedAt >= '${reqBody.from}' AND tbl_buyer_required_lc_limit.updatedAt <= '${reqBody.to}'  `
    let dateRangeInvFin = ` AND tbl_invoice_discounting.modified_at >= '${reqBody.from}' AND tbl_invoice_discounting.modified_at <= '${reqBody.to}'`
    query = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDisbursed FROM tbl_disbursement_scheduled
    LEFT JOIN tbl_invoice_discounting ON 
    tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
    LEFT JOIN tbl_buyer_required_lc_limit ON
    tbl_buyer_required_lc_limit.id = tbl_disbursement_scheduled.invRefNo
    LEFT JOIN tbl_user ON
    tbl_user.id = COALESCE(tbl_invoice_discounting.seller_id, tbl_buyer_required_lc_limit.createdBy)
    WHERE tbl_disbursement_scheduled.status = 1 ${dateRangeInvFin} ${extraQuery}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["totalDisbursed"] = dbRes.message?.[0]?.["totalDisbursed"] || 0

    // Clients onboarded, active & inactive
    query = `SELECT * FROM tbl_user WHERE type_id = 19 AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' ${extraQueryOnboard}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["clientsOnboarded"] = dbRes.message.length

    query = `SELECT * FROM tbl_user WHERE type_id = 19 AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' AND last_login_at >= '${lastActiveDateStr}' ${extraQueryOnboard}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["activeUsers"] = dbRes.message.length
    response["inactiveUsers"] = (response["clientsOnboarded"] - response["activeUsers"])


    // LC applications - limit & finance
    query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_user.id = tbl_buyer_required_lc_limit.createdBy  WHERE (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 0 OR tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 0) ${dateRangeQueryForLC} ${extraQuery}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["lcLimitApplications"] = dbRes.message.length
    query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_user.id = tbl_buyer_required_lc_limit.createdBy WHERE tbl_buyer_required_lc_limit.invRefNo IS NOT NULL  ${dateRangeQueryForLC} ${extraQuery}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["lcFinanceApplications"] = dbRes.message.length

    // Invoice applications - limit & finance
    query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit  
    LEFT JOIN tbl_user ON
    tbl_buyer_required_limit.userId = tbl_user.id WHERE (tbl_buyer_required_limit.termSheetSignedByExporter = 0 OR tbl_buyer_required_limit.termSheetSignedByBank = 0) ${dateRangeQueryForInvoice} ${extraQuery}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["invoiceLimitApplications"] = dbRes.message.length
    let filterQuery = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    FROM tbl_invoice_discounting 
    LEFT JOIN tbl_buyer_required_limit ON
    tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    LEFT JOIN tbl_buyers_detail ON
    tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    LEFT JOIN tbl_user_details supplierDetails ON
    tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    LEFT JOIN tbl_user_details lenderDetails ON
    tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    LEFT JOIN tbl_user ON
    tbl_buyer_required_limit.userId = tbl_user.id
    WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) ${dateRangeInvFin} ${extraQuery}`;
    let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
    response["invoiceFinanceApproved"] = filterDbRes.message.length

    query = `SELECT tbl_invoice_discounting.id FROM tbl_invoice_discounting     
    LEFT JOIN tbl_user ON
    tbl_invoice_discounting.seller_id = tbl_user.id WHERE 1 ${dateRangeInvFin} ${extraQuery}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["invoiceFinanceApplications"] = dbRes.message.length

    // Exporter, importer, financier & channel partner onboarded, active & inactive by custom date
    let pieDataForUserOnboarded = []
    let tableDataForUserOnboarded = []

    let expSummary = { type: "Exporter", value: 0, active: 0, inActive: 0 }
    let importerSummary = { type: "Importer", value: 0, active: 0, inActive: 0 }
    let financierSummary = { type: "Financier", value: 0, active: 0, inActive: 0 }
    let channelPartnerSummary = { type: "Channel Partner", value: 0, active: 0, inActive: 0 }

    if (reqBody.from && reqBody.to) {
      // For exporters
      query = `SELECT * FROM tbl_user WHERE type_id = 19 AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' ${extraQueryOnboard}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      expSummary["value"] = dbRes.message.length

      query = `SELECT * FROM tbl_user WHERE type_id = 19 AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' 
      AND last_login_at >= '${lastActiveDateStr}' ${extraQueryOnboard}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      expSummary["active"] = dbRes.message.length
      expSummary["inActive"] = (expSummary["value"] - expSummary["active"])
      
      // For Financiers
      query = `SELECT * FROM tbl_user WHERE type_id = 8 AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' ${extraQueryOnboard}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      financierSummary["value"] = dbRes.message.length
      
      query = `SELECT * FROM tbl_user WHERE type_id = 8 AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' 
      AND last_login_at >= '${lastActiveDateStr}' ${extraQueryOnboard}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      financierSummary["active"] = dbRes.message.length
      financierSummary["inActive"] = (financierSummary["value"] - financierSummary["active"])
      

      // For CPS
      query = `SELECT * FROM tbl_user WHERE domain_key LIKE '%20%' AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' ${extraQueryOnboard}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      channelPartnerSummary["value"] = dbRes.message.length
      
      query = `SELECT * FROM tbl_user WHERE domain_key LIKE '%20%' AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}'
      AND last_login_at >= '${lastActiveDateStr}' ${extraQueryOnboard}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      channelPartnerSummary["active"] = dbRes.message.length
      channelPartnerSummary["inActive"] = (channelPartnerSummary["value"] - channelPartnerSummary["active"])
      

      pieDataForUserOnboarded.push(expSummary)
      pieDataForUserOnboarded.push(importerSummary)
      pieDataForUserOnboarded.push(financierSummary)
      pieDataForUserOnboarded.push(channelPartnerSummary)

      tableDataForUserOnboarded.push(["Active Users", expSummary["active"], importerSummary["active"], financierSummary["active"], channelPartnerSummary["active"]])
      tableDataForUserOnboarded.push(["Inactive Users", expSummary["inActive"], importerSummary["inActive"], financierSummary["inActive"], channelPartnerSummary["inActive"]])

      response["totalUsersOnboarded"] = expSummary["value"] + importerSummary["value"] + financierSummary["value"] + channelPartnerSummary[["value"]]
      response["tableDataForUserOnboarded"] = tableDataForUserOnboarded
      response["pieDataForUserOnboarded"] = pieDataForUserOnboarded
    }

    // LC & Invoice Application approved, inprogress & rejected status
    let tableDataForInvoiceLcApplication = []

    let invSummary = { type: "Invoice Application", totalApplication: 0, approved: 0, approvedAmount: 0, rejected:0, rejectedAmount: 0,
    pending:0, pendingAmount: 0}
    let lcSummary = { type: "LC Application", totalApplication: 0, approved: 0, approvedAmount: 0, rejected:0, rejectedAmount: 0,
    pending:0, pendingAmount: 0}

    if (reqBody.from && reqBody.to) {
      let dateRangeQueryForInvoice = ` AND tbl_buyer_required_limit.updatedAt >= '${reqBody.from}' AND tbl_buyer_required_limit.updatedAt <= '${reqBody.to}'  `
      let dateRangeQueryForLC = ` AND tbl_buyer_required_lc_limit.updatedAt >= '${reqBody.from}' AND tbl_buyer_required_lc_limit.updatedAt <= '${reqBody.to}'  `

      // Inprogress
      let havingSearchQryInv = ` (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) `
      let extraSearchQryInv = ` AND (tbl_buyer_required_limit.termSheetSignedByExporter = 0 OR 
          tbl_buyer_required_limit.termSheetSignedByBank = 0) `
      let filterQueryInv = `SELECT tbl_buyers_detail.id,
          supplierDetails.company_name AS supplierName,
          GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
          (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
          COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
          tbl_buyer_required_limit.requiredLimit
          FROM tbl_buyers_detail
          INNER JOIN tbl_countries 
          ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
          LEFT JOIN tbl_buyer_required_limit ON
          tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId
          LEFT JOIN tbl_share_invoice_quote_request ON
          tbl_buyer_required_limit.id = tbl_share_invoice_quote_request.quoteId
          LEFT JOIN tbl_user_details ON
          tbl_share_invoice_quote_request.lenderId = tbl_user_details.tbl_user_id
          LEFT JOIN tbl_user_details supplierDetails ON
          tbl_buyer_required_limit.userId = supplierDetails.tbl_user_id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_limit.userId = tbl_user.id
          WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
          ${extraSearchQryInv} ${dateRangeQueryForInvoice} ${extraQuery}
          GROUP BY tbl_share_invoice_quote_request.quoteId HAVING ${havingSearchQryInv} `;
      let filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
      invSummary["pending"] = filterDbResInv.message.length
      let suminvpending = 0
      for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
        const element = filterDbResInv.message[i]
        if(element.requiredLimit){
          suminvpending += parseInt(element.requiredLimit)
        }
      }  
      invSummary["pendingAmount"] = suminvpending
      // Approved
      extraSearchQryInv = ` AND (tbl_buyer_required_limit.termSheetSignedByExporter = 1 AND 
        tbl_buyer_required_limit.termSheetSignedByBank = 1) `
      filterQueryInv = `SELECT tbl_buyers_detail.id,
          supplierDetails.company_name AS supplierName,
          GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
          (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
          COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
          tbl_buyer_required_limit.requiredLimit
          FROM tbl_buyers_detail
          INNER JOIN tbl_countries 
          ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
          LEFT JOIN tbl_buyer_required_limit ON
          tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId
          LEFT JOIN tbl_share_invoice_quote_request ON
          tbl_buyer_required_limit.id = tbl_share_invoice_quote_request.quoteId
          LEFT JOIN tbl_user_details ON
          tbl_share_invoice_quote_request.lenderId = tbl_user_details.tbl_user_id
          LEFT JOIN tbl_user_details supplierDetails ON
          tbl_buyer_required_limit.userId = supplierDetails.tbl_user_id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_limit.userId = tbl_user.id
          WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
          ${extraSearchQryInv} ${dateRangeQueryForInvoice} ${extraQuery}
          GROUP BY tbl_share_invoice_quote_request.quoteId `;
      filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
      invSummary["approved"] = filterDbResInv.message.length
      let suminvapp = 0
      for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
        const element = filterDbResInv.message[i]
        if(element.requiredLimit){
          suminvapp += parseInt(element.requiredLimit)
        }
      }  
      invSummary["approvedAmount"] = suminvapp
      // Rejected
      havingSearchQryInv = ` (countOfDeniedQuotes = countOfSelectedLender) `
      filterQueryInv = `SELECT tbl_buyers_detail.id,
          supplierDetails.company_name AS supplierName,
          GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
          (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
          COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
          tbl_buyer_required_limit.requiredLimit
          FROM tbl_buyers_detail
          INNER JOIN tbl_countries 
          ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
          LEFT JOIN tbl_buyer_required_limit ON
          tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId
          LEFT JOIN tbl_share_invoice_quote_request ON
          tbl_buyer_required_limit.id = tbl_share_invoice_quote_request.quoteId
          LEFT JOIN tbl_user_details ON
          tbl_share_invoice_quote_request.lenderId = tbl_user_details.tbl_user_id
          LEFT JOIN tbl_user_details supplierDetails ON
          tbl_buyer_required_limit.userId = supplierDetails.tbl_user_id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_limit.userId = tbl_user.id
          WHERE tbl_buyer_required_limit.buyerId IS NOT NULL ${dateRangeQueryForInvoice} ${extraQuery}
          GROUP BY tbl_share_invoice_quote_request.quoteId HAVING ${havingSearchQryInv} `;

      filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
      invSummary["rejected"] = filterDbResInv.message.length
      let suminvrej = 0
      for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
        const element = filterDbResInv.message[i]
        if(element.requiredLimit){
          suminvrej += parseInt(element.requiredLimit)
        }
      }  
      invSummary["rejectedAmount"] = suminvrej


      query = `SELECT tbl_buyers_detail.id,
      supplierDetails.company_name AS supplierName,
      GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
      (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
      COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
      tbl_buyer_required_limit.requiredLimit
      FROM tbl_buyers_detail
      INNER JOIN tbl_countries 
      ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId
      LEFT JOIN tbl_share_invoice_quote_request ON
      tbl_buyer_required_limit.id = tbl_share_invoice_quote_request.quoteId
      LEFT JOIN tbl_user_details ON
      tbl_share_invoice_quote_request.lenderId = tbl_user_details.tbl_user_id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_buyer_required_limit.userId = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      tbl_buyer_required_limit.userId = tbl_user.id
      WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
      ${dateRangeQueryForInvoice} ${extraQuery}
      GROUP BY tbl_share_invoice_quote_request.quoteId`
      dbRes = await call({ query }, 'makeQuery', 'get');
      invSummary["totalApplication"] = dbRes.message.length

      // query = `SELECT COUNT(id), SUM(contract_amount) FROM tbl_invoice_discounting WHERE (status = 3 OR status = 4 OR status = 6) ${dateRangeQueryForInvoice} `
      // dbRes = await call({ query }, 'makeQuery', 'get');
      // invSummary["approved"] = dbRes.message[0]["COUNT(id)"]
      // invSummary["approvedAmount"] = dbRes.message[0]["SUM(contract_amount)"]

      // query = `SELECT COUNT(id), SUM(contract_amount) FROM tbl_invoice_discounting WHERE status = 5 ${dateRangeQueryForInvoice} `
      // dbRes = await call({ query }, 'makeQuery', 'get');
      // invSummary["rejected"] = dbRes.message[0]["COUNT(id)"]
      // invSummary["rejectedAmount"] = dbRes.message[0]["SUM(contract_amount)"]

      // query = `SELECT COUNT(id), SUM(contract_amount) FROM tbl_invoice_discounting WHERE status NOT IN (3,4,5,6) ${dateRangeQueryForInvoice} `
      // dbRes = await call({ query }, 'makeQuery', 'get');
      // invSummary["pending"] = dbRes.message[0]["COUNT(id)"]
      // invSummary["pendingAmount"] = dbRes.message[0]["SUM(contract_amount)"]

      // For LC
      // 0 pending
      // 1 Approved
      // 2 rejected
      // 3 Inprogress disbursement
      // 4 Disbursed

    
      // Inprogress
      let havingSearchQry = ` (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) `
      let  extraSearchQry = ` AND (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 0 OR 
        tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 0) `
      let filterQuery = `SELECT tbl_buyer_required_lc_limit.id,
          (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"denied"', '')))/LENGTH('"status":"denied"') AS countOfDeniedQuotes,
          GROUP_CONCAT(lenderDetails.company_name) AS selectedLenderName,
          COUNT(lenderDetails.company_name REGEXP ',') as countOfSelectedLender,
          tbl_buyer_required_lc_limit.ocrFields
          
          FROM tbl_buyer_required_lc_limit

          LEFT JOIN tbl_share_lc_quote_request ON
          tbl_buyer_required_lc_limit.id = tbl_share_lc_quote_request.quoteId
          LEFT JOIN tbl_user_details lenderDetails ON
          tbl_share_lc_quote_request.lenderId = lenderDetails.tbl_user_id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_lc_limit.createdBy = tbl_user.id
          WHERE 1 
          ${extraSearchQry} ${dateRangeQueryForLC} ${extraQuery}
          GROUP BY tbl_buyer_required_lc_limit.id
          HAVING ${havingSearchQry}`;
      let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      let sum = 0
      for(let i = 0; i<= filterDbRes.message.length - 1; i++){
        const element = filterDbRes.message[i]
        if(element.ocrFields?.["32B2"]){
          sum += parseInt(element.ocrFields["32B2"])
        }
      }
      lcSummary["pending"] = filterDbRes.message.length
      lcSummary["pendingAmount"] = sum

      // Approved
      extraSearchQry = ` (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 1 AND 
        tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 1) `
      filterQuery = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields
      FROM tbl_buyer_required_lc_limit
      LEFT JOIN tbl_user ON
      tbl_buyer_required_lc_limit.createdBy = tbl_user.id
      WHERE ${extraSearchQry} ${dateRangeQueryForLC} ${extraQuery}` ;
      filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      lcSummary["approved"] = filterDbRes.message.length
      let sum2 = 0
      for(let i = 0; i<= filterDbRes.message.length - 1; i++){
        const element = filterDbRes.message[i]
        if(element.ocrFields["32B2"]){
          sum2 += parseInt(element.ocrFields["32B2"])
        }
      }
      lcSummary["approvedAmount"] = sum2
      // Rejected
      havingSearchQry = ` (countOfDeniedQuotes = countOfSelectedLender) `
      filterQuery = `SELECT tbl_buyer_required_lc_limit.id,
      (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"denied"', '')))/LENGTH('"status":"denied"') AS countOfDeniedQuotes,
      GROUP_CONCAT(lenderDetails.company_name) AS selectedLenderName,
      COUNT(lenderDetails.company_name REGEXP ',') as countOfSelectedLender,tbl_buyer_required_lc_limit.ocrFields 
      
      FROM tbl_buyer_required_lc_limit

      LEFT JOIN tbl_share_lc_quote_request ON
      tbl_buyer_required_lc_limit.id = tbl_share_lc_quote_request.quoteId
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_share_lc_quote_request.lenderId = lenderDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      tbl_buyer_required_lc_limit.createdBy = tbl_user.id
      WHERE 1 ${dateRangeQueryForLC} ${extraQuery}
      GROUP BY tbl_buyer_required_lc_limit.id 
      HAVING ${havingSearchQry}`;

      filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      lcSummary["rejected"] = filterDbRes.message.length
      let sum3 = 0
      for(let i = 0; i<= filterDbRes.message.length - 1; i++){
        const element = filterDbRes.message[i]
        if(element.ocrFields["32B2"]){
          sum3 += parseInt(element.ocrFields["32B2"])
        }
      }
      lcSummary["rejectedAmount"] = sum3

      query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit 
      LEFT JOIN tbl_user ON
      tbl_buyer_required_lc_limit.createdBy = tbl_user.id

      WHERE 1 ${dateRangeQueryForLC} ${extraQuery}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      lcSummary["totalApplication"] = dbRes.message.length

      response["lcSummary"] = lcSummary
      response["invSummary"] = invSummary

      tableDataForInvoiceLcApplication.push(["LC Discounting",  lcSummary["approved"], (lcSummary["approvedAmount"] || 0), 
          lcSummary["rejected"], (lcSummary["rejectedAmount"] || 0), lcSummary["pending"], (lcSummary["pendingAmount"] || 0)])

      tableDataForInvoiceLcApplication.push(["Invoice Discounting", invSummary["approved"], (invSummary["approvedAmount"] || 0), 
          invSummary["rejected"], (invSummary["rejectedAmount"] || 0), invSummary["pending"], (invSummary["pendingAmount"] || 0) ])

      response["tableDataForInvoiceLcApplication"] = tableDataForInvoiceLcApplication
    }
    res.send({
      success: true,
      message: response
    })
  }
  catch (error) {
    console.log("in getAdminDashboardStats error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getTodaysUpdateForAdminDashboard = async (req, res, next) => {
  try {
    let reqBody = req.body
    let query = ""
    let dbRes = null
    let dataToReturn = []

    // Today onboarded users who didnt add any buyers
    query = `SELECT 
    tbl_user_details.company_name AS supplierName,
    supplierCountry.name AS supplierCountryName
    
    FROM tbl_user_details

    LEFT JOIN tbl_user ON 
    tbl_user_details.tbl_user_id = tbl_user.id

    LEFT JOIN tbl_buyers_detail ON
    tbl_user_details.tbl_user_id = tbl_buyers_detail.user_id

    LEFT JOIN tbl_countries supplierCountry ON
    tbl_user_details.country_code = supplierCountry.sortname

    WHERE tbl_user_details.created_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' 
    AND tbl_buyers_detail.id IS NULL AND tbl_user.type_id = 19 
    GROUP BY tbl_user_details.tbl_user_id`
    dbRes = await call({ query }, 'makeQuery', 'get');
    dataToReturn = dataToReturn.concat(dbRes.message)

    // User who added buyer today and those buyer didnt applied for limit yet
    query = `SELECT tbl_buyers_detail.buyerName,
    tbl_buyers_detail.id AS buyerId,
    tbl_user_details.company_name AS supplierName,
    supplierCountry.name AS supplierCountryName
    
    FROM tbl_buyers_detail 
    
    LEFT JOIN tbl_buyer_required_limit ON
    tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId

    LEFT JOIN tbl_user_details ON
    tbl_buyers_detail.user_id = tbl_user_details.tbl_user_id

    LEFT JOIN tbl_countries supplierCountry ON
    tbl_user_details.country_code = supplierCountry.sortname

    WHERE tbl_buyers_detail.created_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}'
    AND tbl_buyer_required_limit.buyerId IS NULL 
    
    GROUP BY tbl_buyers_detail.id`
    dbRes = await call({ query }, 'makeQuery', 'get');
    dataToReturn = dataToReturn.concat(dbRes.message)   

    res.send({
      success: true,
      message: dataToReturn
    })
  }
  catch (error) {
    console.log("error in getTodaysUpdateForAdminDashboard", error);
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getFilterForInvoiceBill = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filterData = {}
    //
    filterData["Status"] = {
      "accordianId": 'status',
      type: "checkbox",
      labelName: "name",
      data: [{name: "Due"},{name: "Paid"}, {name: 'Over Due'}]
    }
    //
    filterData["InvoiceDate"] = {
      "accordianId": 'dateRangeFilter',
      type: "minMaxDate",
      value: []
    }
    //
    filterData["Financier"] = {
      "accordianId": 'financier',
      type: "checkbox",
      labelName: "name",
      data: await emailEnabledBanks()
    }
    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("error in getFilterForInvoiceBill", error);
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getCPInvoiceDetails = async (req, res, next) => {
  try {
    let reqBody = req.body
    let todayDateObj = moment()
    let query = ""
    let dbRes = null
    let response = {
      commissionFromCPDbData: []
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
            extraSearchQry = ` AND tbl_network_requests.request_from IN (${reqBody.generateInvoiceForId.join(",")})`
          }

          // For Invoice
          query = `SELECT 
          tbl_invoice_discounting.created_at AS applicationCreatedAt,
          tbl_invoice_discounting.id AS applicationId, tbl_invoice_discounting.charges AS invCharges, tbl_invoice_discounting.commissionStatus AS invoiceCommissionStatus,
          tbl_invoice_discounting.commissionDate,
          lenderDetails.tbl_user_id AS lenderId, lenderDetails.company_name AS lenderName, lenderDetails.user_address AS lenderAddress, 
          sellerDetails.company_name AS sellerName, sellerDetails.user_address AS sellerAddress, 
          tbl_invoice_discounting.due_date AS invoiceDueDate , tbl_invoice_discounting.contract_amount AS contractAmount,
          tbl_mst_currency.code AS contractAmountCurrency, tbl_invoice_discounting.reference_no AS applicationNo,
          cpDetails.company_name AS cpCompanyName, cpDetails.user_address AS cpAddress

          FROM tbl_invoice_discounting 

          LEFT JOIN tbl_user_details lenderDetails ON
          tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id

          LEFT JOIN tbl_network_requests ON 
          tbl_network_requests.request_to = tbl_invoice_discounting.seller_id

          LEFT JOIN tbl_user_details cpDetails ON
          tbl_network_requests.request_from = cpDetails.tbl_user_id

          LEFT JOIN tbl_user_details sellerDetails ON
          tbl_invoice_discounting.seller_id = sellerDetails.tbl_user_id

          LEFT JOIN tbl_mst_currency ON
          tbl_invoice_discounting.currency = tbl_mst_currency.id
          
          WHERE tbl_invoice_discounting.charges IS NOT NULL AND tbl_invoice_discounting.due_date BETWEEN '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND (tbl_invoice_discounting.status IN (3, 4, 6)) ${extraSearchQry} `
          dbRes = await call({ query }, 'makeQuery', 'get');
          tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)
          // console.log("tempArrayToStoreCharges", query);

          // For LC not tested
          // query = `SELECT 
          // tbl_buyer_required_lc_limit.createdAt AS applicationCreatedAt, tbl_buyer_required_lc_limit.id AS applicationId, tbl_buyer_required_lc_limit.charges AS lcCharges, tbl_buyer_required_lc_limit.commissionStatus AS lcCommissionStatus,
          // tbl_buyer_required_lc_limit.commissionDate,
          // lenderDetails.tbl_user_id AS lenderId, lenderDetails.company_name AS lenderName, lenderDetails.user_address AS lenderAddress,
          // sellerDetails.company_name AS sellerName, sellerDetails.user_address AS sellerAddress, 
          // tbl_buyer_required_lc_limit.invoiceDueDate , tbl_buyer_required_lc_limit.contractAmount,
          // tbl_buyer_required_lc_limit.contractAmountCurrency, tbl_buyer_required_lc_limit.invRefNo AS applicationNo,
          // cpDetails.company_name AS cpCompanyName, cpDetails.user_address AS cpAddress
          
          // FROM tbl_buyer_required_lc_limit 

          // LEFT JOIN tbl_user_details lenderDetails ON
          // tbl_buyer_required_lc_limit.selectedFinancier = lenderDetails.tbl_user_id

          // LEFT JOIN tbl_network_requests ON 
          // tbl_network_requests.request_to = tbl_buyer_required_lc_limit.userId

          // LEFT JOIN tbl_user_details cpDetails ON
          // tbl_network_requests.request_from = cpDetails.tbl_user_id

          // LEFT JOIN tbl_user_details sellerDetails ON
          // tbl_buyer_required_lc_limit.createdBy = sellerDetails.tbl_user_id
          
          // WHERE tbl_buyer_required_lc_limit.charges IS NOT NULL AND tbl_buyer_required_lc_limit.invoiceDueDate BETWEEN '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
          // '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND tbl_buyer_required_lc_limit.financeStatus IN (1, 3, 4) ${extraSearchQry} `

          // dbRes = await call({ query }, 'makeQuery', 'get');
          // tempArrayToStoreCharges = tempArrayToStoreCharges.concat(dbRes.message)

          for (let j = 0; j < tempArrayToStoreCharges.length; j++) {
            const {lenderName, invCharges, invoiceCommissionStatus, lcCharges, 
              lcCommissionStatus,applicationNo, contractAmount, contractAmountCurrency,invoiceDueDate } = tempArrayToStoreCharges[j];

            response["commissionFromCPDbData"].push(tempArrayToStoreCharges[j])
          }
          tempArrayToStoreCharges = []
        }
      }
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
    console.log("error in getCPInvoiceDetails", error);
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

exports.getCPListWithCommission = async (req, res, next) => {
  try {
    let reqBody = req.body
    let query = `SELECT tbl_user_details.company_name AS name,
    tbl_user_details.tbl_user_id as id,
    tbl_cp_commission_rate.commissionCharges FROM tbl_user 
    LEFT JOIN tbl_user_details ON
    tbl_user.id = tbl_user_details.tbl_user_id
    LEFT JOIN tbl_cp_commission_rate ON
    tbl_user.id = tbl_cp_commission_rate.userId
    WHERE tbl_user.domain_key LIKE '%20%' AND tbl_user.status = 1 AND tbl_cp_commission_rate.userId IS NOT NULL `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    if(res?.send){
      res.send({
        success: true,
        message: dbRes.message
      })
    }
    else{
      return({
        success: true,
        message: dbRes.message
      })
    }
  }
  catch (error) {
    console.log("error in getCPListWithCommission", error);
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

exports.uploadInvoiceAttachment = async (req, res, next) => {
  try {
    let reqBody = req.body
    let reqFiles = req.files
    fs.writeFileSync('./docs/' + reqFiles["file"].md5, reqFiles["file"].data);
    let dbRes = await dbPool.query(`INSERT INTO tbl_document_details (doc_name, file_name, gen_doc_label, file_hash,
          created_at, created_by, modified_at ) VALUE ("Invoice Attachment", "${reqFiles["file"].name}", "Invoice Attachment",
          "${reqFiles["file"].md5}", "${getCurrentTimeStamp()}", "${reqBody.userId}", "${getCurrentTimeStamp()}") `)
    await dbPool.query(`UPDATE tbl_invoice_billing SET attachment = '${dbRes[0]["insertId"]}' WHERE billNo = '${reqBody.billNo}' `)
    res.send({
      success: true,
      message: "Document uploaded."
    })
  }
  catch (error) {
    console.log("error in uploadInvoiceAttachment", error)
    res.send({
      success: false,
      message: error
    })
  }
}

