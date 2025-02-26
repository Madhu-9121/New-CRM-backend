const { dbPool } = require("../../src/database/mysql")
const { call } = require("../../utils/codeHelper")
const moment = require("moment");
const fs = require('fs');
const e = require("cors");
const { nullCheck } = require("../../database/utils/utilFuncs");
const { count } = require("console");
const { sendMail } = require("../../utils/mailer");
const config = require("../../config");
const { emailEnabledBanks, mongoConnectionString, environment } = require("../../urlCostants");
const { getCurrentTimeStamp, getFinancialYearDateRange, formatSqlQuery } = require("../../iris_server/utils");
const { MongoClient } = require("mongodb");

exports.getAllAvailableTermSheets = async (req, res, next) => {
  try {
    let reqBody = req.body
    let resp = []
    let query = `SELECT tbl_termsheet_mapper.tbl_doc_id,
    tbl_user_details.company_name,
    tbl_user_details.tbl_user_id AS lender_id 
    FROM tbl_termsheet_mapper 
    LEFT JOIN tbl_user_details ON
    tbl_termsheet_mapper.finacerId = tbl_user_details.tbl_user_id
    WHERE tbl_termsheet_mapper.application_id = '${reqBody.applicationId}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    resp = dbRes.message

    query =  `SELECT tbl_buyer_required_limit.termSheet AS tbl_doc_id,
    tbl_user_details.company_name,
    tbl_user_details.tbl_user_id AS lender_id  
    FROM tbl_buyer_required_limit
    LEFT JOIN tbl_user_details ON 
    tbl_buyer_required_limit.selectedFinancier = tbl_user_details.tbl_user_id 
    WHERE tbl_buyer_required_limit.termSheet IS NOT NULL AND tbl_buyer_required_limit.selectedFinancier IS NOT NULL
    AND tbl_buyer_required_limit.id = '${reqBody.applicationId}' `
    dbRes = await call({ query }, 'makeQuery', 'get');

    if(dbRes.message?.length){
      resp.push(dbRes.message[0])
    }
    res.send({
      success: true,
      message: resp
    })
  }
  catch (error) {
    console.log("in getAllAvailableTermSheets error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.manageInvoiceDiscountingLogs = async (invRefNo, status) => {
  return new Promise(async (resolve) => {
    // 3 - approved
    // 5 - rejected
    // 4 - disbursed
    // 6 - In progress

    // 11 - Agreement sent
    // 12 - Agreement sign by exporter
    // 13 - Agreement sign by buyer
    // 14 - Agreement sign by financier

    // Check if alredy exist then update
    let query = `SELECT * FROM tbl_invoice_discounting_audit_logs WHERE invRefNo = '${invRefNo}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    if (dbRes.message.length) {
      query = ` UPDATE tbl_invoice_discounting_audit_logs SET `
      let subQuery = ""
      if (status / 1 == 3) {
        subQuery = ` approvedOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 5){
        subQuery = ` rejectedOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 4){
        subQuery = ` disbursedOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 6){
        subQuery = ` inprogressOn = '${getCurrentTimeStamp()}' `
      }
      // agreement timestamp
      if (status / 1 == 11){
        subQuery = ` agreementSentOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 12){
        subQuery = ` agreementSignByExporter = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 13){
        subQuery = ` agreementSignByBuyer = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 14){
        subQuery = ` agreementSignByFinancier = '${getCurrentTimeStamp()}' `
      }
      await dbPool.query(query + subQuery)
      return resolve(true)
    }
    // if not exist create and update
    else {
      query = ` INSERT INTO tbl_invoice_discounting_audit_logs `
      let subQuery = ""
      if (status / 1 == 3) {
        subQuery = ` (invRefNo, approvedOn) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 5){
        subQuery = ` (invRefNo, rejectedOn) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 4){
        subQuery = ` (invRefNo, disbursedOn) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 6){
        subQuery = ` (invRefNo, inprogressOn) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      // agreement timestamp
      if (status / 1 == 11){
        subQuery = ` (invRefNo, agreementSentOn) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 12){
        subQuery = ` (invRefNo, agreementSignByExporter) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 13){
        subQuery = ` (invRefNo, agreementSignByBuyer) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 14){
        subQuery = ` (invRefNo, agreementSignByFinancier) VALUES ('${invRefNo}', '${getCurrentTimeStamp()}') `
      }
      await dbPool.query(query + subQuery)
      return resolve(true)
    }
  })
}

function updateCountQyeryAndReturn(userId, searchQuery){
  return `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS sumAmountInUSD
  FROM tbl_invoice_discounting 
  LEFT JOIN tbl_buyer_required_limit ON
  tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
  LEFT JOIN tbl_buyers_detail ON
  tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
  LEFT JOIN tbl_user_details supplierDetails ON
  tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
  LEFT JOIN tbl_user ON
  supplierDetails.tbl_user_id = tbl_user.id
  LEFT JOIN tbl_user_details adminDetails ON
  adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
  LEFT JOIN tbl_user_details lenderDetails ON
  tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
  LEFT JOIN tbl_disbursement_scheduled ON
  tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
  WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) 
  AND supplierDetails.tbl_user_id = ${userId} ${searchQuery} `
}

exports.getIDApprovedApplicationCounts = async (req, res, next) => {
  try {
    let {userId} = req.body
    let result = {}
    let dbRes = null

    let havingSearchQry = ""
    let searchQuery = ""
    let extraSearchQry = ""

    searchQuery = ` AND tbl_disbursement_scheduled.createdAt >= '${moment().subtract(12, "weeks").format("YYYY-MM-DD")}' AND 
    tbl_disbursement_scheduled.createdAt <= '${moment().format("YYYY-MM-DD")}' AND tbl_disbursement_scheduled.status = 0 AND 
    tbl_disbursement_scheduled.scheduledOn > '${moment().format("YYYY-MM-DD")}' `

    let countQuery = updateCountQyeryAndReturn(userId, searchQuery, extraSearchQry, havingSearchQry)
    dbRes = await call({query: countQuery}, 'makeQuery', 'get');
    result["due12Week"] = dbRes.message?.[0]?.["sumAmountInUSD"] || 0  

    searchQuery = ` AND tbl_disbursement_scheduled.createdAt >= '${moment().subtract(12, "weeks").format("YYYY-MM-DD")}' AND 
    tbl_disbursement_scheduled.createdAt <= '${moment().format("YYYY-MM-DD")}' AND tbl_disbursement_scheduled.status = 0 AND 
    tbl_disbursement_scheduled.scheduledOn < '${moment().format("YYYY-MM-DD")}' `

    countQuery = updateCountQyeryAndReturn(userId, searchQuery, extraSearchQry, havingSearchQry)
    dbRes = await call({query: countQuery}, 'makeQuery', 'get');
    result["overdue12Week"] = dbRes.message?.[0]?.["sumAmountInUSD"] || 0

    searchQuery = ` AND tbl_disbursement_scheduled.createdAt >= '${moment().subtract(12, "weeks").format("YYYY-MM-DD")}' AND 
    tbl_disbursement_scheduled.createdAt <= '${moment().format("YYYY-MM-DD")}' AND tbl_disbursement_scheduled.status = 1 `

    countQuery = updateCountQyeryAndReturn(userId, searchQuery, extraSearchQry, havingSearchQry)
    dbRes = await call({query: countQuery}, 'makeQuery', 'get');
    result["received12Week"] = dbRes.message?.[0]?.["sumAmountInUSD"] || 0

    searchQuery = ` AND tbl_disbursement_scheduled.status = 1 `

    countQuery = updateCountQyeryAndReturn(userId, searchQuery, extraSearchQry, havingSearchQry)
    dbRes = await call({query: countQuery}, 'makeQuery', 'get');
    result["receivable"] = dbRes.message?.[0]?.["sumAmountInUSD"] || 0

    searchQuery = ` AND tbl_disbursement_scheduled.status = 0 AND tbl_disbursement_scheduled.scheduledOn < '${moment().format("YYYY-MM-DD")}' `

    countQuery = updateCountQyeryAndReturn(userId, searchQuery, extraSearchQry, havingSearchQry)
    dbRes = await call({query: countQuery}, 'makeQuery', 'get');
    result["overdue"] = dbRes.message?.[0]?.["sumAmountInUSD"] || 0  

    res.send({
      success: true,
      data: result
    })    
  } catch (error) {
    console.log("erroringetIDApprovedApplicationCounts", error);    
    res.send({
      success: true,
      data: {}
    })
  }
}

exports.getInvoiceDiscountingListForAdmin = async (req, res, next) => {
  try {
    let reqBody = req.body
    if(reqBody.atLimitStage){

      let sortString = ` ORDER BY tbl_buyer_required_limit.createdAt DESC `
      let havingSearchQry = " HAVING " 
      let searchQuery = ""
      let perPageString = "";
      let extraSearchQry = "";

      if(reqBody.buyerName){
        extraSearchQry += ` AND tbl_buyers_detail.buyerName IN (${reqBody.buyerName.join(",")}) `
      }

      if(reqBody.exporterName){
        extraSearchQry += ` AND supplierDetails.company_name IN (${reqBody.exporterName.join(",")}) `
      }

      if(reqBody.admins){
        extraSearchQry += ` AND adminDetails.contact_person IN (${reqBody.admins.join(",")}) `
      }

      if (reqBody.dateRangeFilter) {
        if (reqBody.dateRangeFilter[0] && !reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_buyer_required_limit.createdAt >= '${reqBody.dateRangeFilter[0]}' `
        }
        else if (!reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_buyer_required_limit.createdAt <= '${reqBody.dateRangeFilter[1]}'  `
        }
        else if (reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_buyer_required_limit.createdAt BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' `
        }
      }

      if(reqBody.onlyShowForUserId){
        extraSearchQry += ` AND ( tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}')`
      }
      if(reqBody.subadminIds){
        extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }
      
      if (reqBody.financiersFilter) {
        let lastIndex = reqBody.financiersFilter.length - 1
        for (let index = 0; index < reqBody.financiersFilter.length; index++) {
          const element = reqBody.financiersFilter[index];
          if(index == 0){
            havingSearchQry += ` ( ` 
          }
          havingSearchQry += ` selectedLenderName LIKE '%${element}%' ${lastIndex!=index ? ' OR ' : ''} `
          if (lastIndex == index){
            havingSearchQry += ` ) `
          }
        }
      }
      if(havingSearchQry===" HAVING "){
        havingSearchQry = ""
      }
      if(reqBody.status){
        let isUnderReview = reqBody.status.includes("Under Review")
        let isRejected = reqBody.status.includes("Rejected")
        let isApproved = reqBody.status.includes("Approved")
        let isInprogress = reqBody.status.includes("Inprogress")
        let isExpired = reqBody.status.includes("Expired")
        if(isRejected){
          havingSearchQry = !havingSearchQry ? " HAVING " : (havingSearchQry )
          if(havingSearchQry!=" HAVING "){
            havingSearchQry += ` ${reqBody.status.length/1 == 1  ? ' AND ' : ' OR ' } `
          }
          havingSearchQry += ` (countOfDeniedQuotes = countOfSelectedLender) `
        }
        if(isApproved){
          extraSearchQry += ` ${reqBody.status.length/1 == 1 ? ' AND ' : ' OR '} (tbl_buyer_required_limit.termSheetSignedByExporter = 1 AND 
          tbl_buyer_required_limit.termSheetSignedByBank = 1) `
        }
        if(isUnderReview){
          extraSearchQry += ` ${reqBody.status.length/1 == 1 ? ' AND ' : ' OR '} (tbl_buyer_required_limit.adminReview IS NULL) `
        }
        if(isInprogress){
          havingSearchQry = !havingSearchQry ? " HAVING " : (havingSearchQry )
          if(havingSearchQry!=" HAVING "){
            havingSearchQry += ` ${reqBody.status.length/1 == 1  ? ' AND ' : ' OR ' } `
          }
          havingSearchQry += ` (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) `
          extraSearchQry += ` AND (tbl_buyer_required_limit.termSheetSignedByExporter = 0 OR 
          tbl_buyer_required_limit.termSheetSignedByBank = 0) AND 
          tbl_buyer_required_limit.createdAt > '${moment().subtract(60, "days").format("YYYY-MM-DD")}' `
        }
        if(isExpired){
          extraSearchQry += ` AND tbl_buyer_required_limit.createdAt <= '${moment().subtract(60, "days").format("YYYY-MM-DD")}'  AND 
          (tbl_buyer_required_limit.termSheetSignedByExporter != 1 OR tbl_buyer_required_limit.termSheetSignedByBank != 1) `
          havingSearchQry = !havingSearchQry ? " HAVING (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) " :
          " (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) "
        }
      }
      if(havingSearchQry===" HAVING "){
        havingSearchQry = ""
      }
      if(reqBody.search){
        searchQuery = ` AND (tbl_buyers_detail.buyerName LIKE '%${reqBody.search}%' OR supplierDetails.company_name LIKE '%${reqBody.search}%' ) `
      }

      if(reqBody.resultPerPage && reqBody.currentPage) {
        perPageString = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage}`;
      } 
      
      if(reqBody.sortDateBy){
        sortString = ` ORDER BY tbl_buyer_required_limit.createdAt ${reqBody.sortDateBy} `
      }
      
      if(reqBody.sortExpName){
        sortString = ` ORDER BY supplierDetails.company_name ${reqBody.sortExpName} `
      }

      if(reqBody.sortBuyerName){
        sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortBuyerName} `
      }
      

      let query = `SELECT 
      tbl_buyers_detail.id, tbl_buyers_detail.identifier, tbl_buyers_detail.stenn_id, tbl_buyers_detail.user_id, 
      tbl_buyers_detail.buyerName, tbl_buyers_detail.buyerEmail,tbl_buyers_detail.buyerPhone,tbl_buyers_detail.buyerAddress,
      tbl_buyers_detail.buyerDUNSNo, tbl_buyers_detail.buyerWebsite, tbl_buyers_detail.buyerCurrency,
      tbl_buyers_detail.buyersDocId, tbl_buyers_detail.previousAnnualSale, tbl_buyers_detail.currentAnnualSale,
      tbl_buyers_detail.buyerCountry, tbl_buyers_detail.buyerPosition, tbl_buyers_detail.nameAuthorizedSignatory,
      tbl_buyers_detail.incoTerms, tbl_buyers_detail.productDetails,
      tbl_buyers_detail.termsOfPayment,
      tbl_buyers_detail.buyersAPIDetail,
      tbl_buyers_detail.buyerNameTitle,
      tbl_buyers_detail.lead_by,
      tbl_buyers_detail.buyerPhoneCode,
      tbl_buyers_detail.buyerHsnCode,
      tbl_buyers_detail.buyerOtherDocs,
      tbl_buyers_detail.ttvId, 
      tbl_buyers_detail.annualTurnOver,
      tbl_buyers_detail.isPromotedToEdit,
      tbl_buyers_detail.ActionBy,
      tbl_buyers_detail.created_by,
      tbl_buyer_required_limit.adminReview,
      tbl_buyer_required_limit.buyers_credit,
      tbl_buyer_required_limit.buyersRemark, 
      tbl_countries.name AS countryName,
      (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
      COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
      tbl_buyer_required_limit.id AS applicationId, tbl_buyer_required_limit.expShipmentDate,
      tbl_buyer_required_limit.invRefNo,
      tbl_buyer_required_limit.updatedAt AS applicationUpdatedAt,
      tbl_buyer_required_limit.createdAt AS applicationCreatedAt,   
      tbl_buyer_required_limit.termSheet, 
      tbl_buyer_required_limit.termSheetSignedByExporter,
      tbl_buyer_required_limit.termSheetSignedByBank,  
      tbl_buyer_required_limit.documentStatus,
      tbl_buyer_required_limit.requiredLimit,
      tbl_buyer_required_limit.currency AS requiredLimitCurrency,
      supplierDetails.company_name AS supplierName,
      supplierDetails.email_id AS supplierEmailId,
      supplierCountry.name AS supplierCountryName,
      buyerCountry.name AS buyerCountryName,
      supplierDetails.organization_type,
      supplierDetails.iec_no AS supplierIecNo,
      adminDetails.contact_person AS leadAssignToName,
      adminDetails.tbl_user_id AS leadAssignToId,
      GROUP_CONCAT(DISTINCT tbl_user_details.company_name ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderName,
      GROUP_CONCAT(DISTINCT tbl_user_details.tbl_user_id ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderId,
      GROUP_CONCAT(tbl_share_invoice_quote_request.isShared ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderSharedStatus,
      supplierDetails.company_city AS supplierCompanyCity,
      supplierDetails.pan_no AS supplierPanNo,
      supplierDetails.industry_type AS supplierIndustryType,
      GROUP_CONCAT(DISTINCT chat_rooms.chat_room_id ORDER BY chat_rooms.chat_room_id SEPARATOR ',') as chatRoomIds,
      GROUP_CONCAT(DISTINCT chat_rooms.included_users ORDER BY chat_rooms.chat_room_id SEPARATOR ',') as chatRoomUsers,
      IFNULL(um.unreadMsgCount, '0') AS chatRoomUnreadMsgCount,

      (SELECT tbl_admin_remarks.remark FROM tbl_admin_remarks
      WHERE tbl_admin_remarks.invApplicationId = tbl_buyer_required_limit.id
      ORDER BY tbl_admin_remarks.id DESC LIMIT 1
      ) AS lastInternalRemark,
      GROUP_CONCAT(DISTINCT IFNULL(tbl_last_message.id, 'null') ORDER BY chat_rooms.chat_room_id) AS lastMessageIds


      FROM tbl_buyers_detail
      INNER JOIN tbl_countries 
      ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId
      LEFT JOIN tbl_chat_rooms AS chat_rooms ON chat_rooms.invApplicationId = tbl_buyer_required_limit.id
      LEFT JOIN tbl_share_invoice_quote_request ON
      tbl_buyer_required_limit.id = tbl_share_invoice_quote_request.quoteId
      LEFT JOIN tbl_user_details ON
      tbl_share_invoice_quote_request.lenderId = tbl_user_details.tbl_user_id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_buyer_required_limit.userId = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_countries supplierCountry ON
      supplierDetails.country_code = supplierCountry.sortname
      LEFT JOIN tbl_countries buyerCountry ON
      tbl_buyers_detail.buyerCountry = buyerCountry.sortname
      LEFT JOIN tbl_admin_remarks ar ON ar.invApplicationId = tbl_buyer_required_limit.id
      LEFT JOIN (
        SELECT cr.invApplicationId, GROUP_CONCAT(IFNULL(um.count, '0') ORDER BY cr.chat_room_id SEPARATOR ',') AS unreadMsgCount
        FROM tbl_chat_rooms cr
        LEFT JOIN tbl_chatroom_unread_msg um ON cr.chat_room_id = um.chatRoomId AND um.userId = ${environment == "prod" ? "1" : "121"}
        GROUP BY cr.invApplicationId
      ) um ON um.invApplicationId = tbl_buyer_required_limit.id   
      LEFT JOIN tbl_last_message ON tbl_last_message.chat_room_id = chat_rooms.chat_room_id    
      WHERE tbl_buyer_required_limit.buyerId IS NOT NULL AND tbl_buyer_required_limit.limitPendingFrom IS NULL
      ${searchQuery} ${extraSearchQry}
      GROUP BY tbl_share_invoice_quote_request.quoteId ${havingSearchQry}
      ${sortString} ${perPageString}`;

      console.log('query atLimitStage =========================>', query);

      let countQuery = `SELECT 
      tbl_buyers_detail.id,
      COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
      (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
      GROUP_CONCAT(DISTINCT tbl_user_details.company_name ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderName,
      GROUP_CONCAT(DISTINCT tbl_user_details.tbl_user_id ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderId
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
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo    
      WHERE tbl_buyer_required_limit.buyerId IS NOT NULL AND tbl_buyer_required_limit.limitPendingFrom IS NULL
      ${searchQuery} ${extraSearchQry}
      GROUP BY tbl_share_invoice_quote_request.quoteId ${havingSearchQry}
      ${sortString} `;
      
      let dbRes = await call({ query }, 'makeQuery', 'get');

      // Memory operation to get FOB value by iec number from mongo db start
      if(dbRes.message.length && reqBody.includeFobInr){
        let client = new MongoClient(mongoConnectionString);
        await client.connect();
        let db = client.db('trade_db');
        let collection = db.collection('tbl_iec_transactions_shipping_bill')
        for (let index = 0; index < dbRes.message.length; index++) {
          const element = dbRes.message[index];
          let monDbRes = await collection.aggregate([
            {
              $match: {
                iec: element.supplierIecNo,
                timestamp: {
                  $gte: getFinancialYearDateRange()["from"],
                  $lte: getFinancialYearDateRange()["to"]
                }
              }
            },
            {
              $group: {
                _id: null,
                fobInr: { $sum: "$shippingBillDetails.fobInr" }
              }
            }
          ]).toArray();
          // console.log("monDbResssssssssssssssssssssssssssss", monDbRes);  
          if(monDbRes?.[0]?.fobInr){
            element["fobInrFromKarzaIec"] = monDbRes?.[0]?.fobInr
          }     
        }
        await client.close()
      }
      // Memory operation to get FOB value by iec number from mongo db end

      let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');

      let filterCount = {}

      // Inprogress
      havingSearchQry = ` (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) `
      extraSearchQry = ` AND (tbl_buyer_required_limit.termSheetSignedByExporter = 0 OR 
          tbl_buyer_required_limit.termSheetSignedByBank = 0) AND tbl_buyer_required_limit.createdAt > CURDATE() - INTERVAL 60 DAY `
      if(reqBody.onlyShowForUserId){
        extraSearchQry += ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
      }
      if(reqBody.subadminIds){
        extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }
          let filterQuery = `SELECT tbl_buyers_detail.id,
          supplierDetails.company_name AS supplierName,
          GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
          (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
          COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender
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
          supplierDetails.tbl_user_id = tbl_user.id
          LEFT JOIN tbl_user_details adminDetails ON
          adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
          WHERE tbl_buyer_required_limit.buyerId IS NOT NULL AND tbl_buyer_required_limit.limitPendingFrom IS NULL
          ${extraSearchQry}
          GROUP BY tbl_share_invoice_quote_request.quoteId HAVING ${havingSearchQry} `;
          let filterDbRes = null
      if(!reqBody.dontFetchCountStats){
        filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
        filterCount["inprogress"] = filterDbRes.message.length
      }

      // Under Review
      filterQuery = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit 
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_buyer_required_limit.userId = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      WHERE tbl_buyer_required_limit.adminReview IS NULL ${extraSearchQry}`      
      if(!reqBody.dontFetchCountStats){
        filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
        filterCount["underreview"] = filterDbRes.message.length
      }

      // Approved
      extraSearchQry = ` AND (tbl_buyer_required_limit.termSheetSignedByExporter = 1 AND 
        tbl_buyer_required_limit.termSheetSignedByBank = 1) `
        if(reqBody.onlyShowForUserId){
          extraSearchQry += ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
        }
        if(reqBody.subadminIds){
          extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
        }
      filterQuery = `SELECT tbl_buyers_detail.id,
          supplierDetails.company_name AS supplierName,
          GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
          (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
          COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender
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
          supplierDetails.tbl_user_id = tbl_user.id
          LEFT JOIN tbl_user_details adminDetails ON
          adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
          WHERE tbl_buyer_required_limit.buyerId IS NOT NULL AND tbl_buyer_required_limit.limitPendingFrom IS NULL
          ${extraSearchQry}
          GROUP BY tbl_share_invoice_quote_request.quoteId `;
              
      if(!reqBody.dontFetchCountStats){
        filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
        filterCount["approved"] = filterDbRes.message.length
      }

      // Rejected
      havingSearchQry = ` (countOfDeniedQuotes = countOfSelectedLender) `
      extraSearchQry = ''
      if(reqBody.onlyShowForUserId){
        extraSearchQry += ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
      }
      if(reqBody.subadminIds){
        extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }
      filterQuery = `SELECT tbl_buyers_detail.id,
          supplierDetails.company_name AS supplierName,
          GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
          (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
          COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender
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
          supplierDetails.tbl_user_id = tbl_user.id
          LEFT JOIN tbl_user_details adminDetails ON
          adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
          WHERE tbl_buyer_required_limit.buyerId IS NOT NULL AND tbl_buyer_required_limit.limitPendingFrom IS NULL
          GROUP BY tbl_share_invoice_quote_request.quoteId HAVING ${havingSearchQry} `;
          
      if(!reqBody.dontFetchCountStats){
        filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
        filterCount["rejected"] = filterDbRes.message.length
      }


      // Expired
      havingSearchQry = ` (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) `
      extraSearchQry = ` AND tbl_buyer_required_limit.createdAt <= '${moment().subtract(60, "days").format("YYYY-MM-DD")}' AND 
      (tbl_buyer_required_limit.termSheetSignedByExporter != 1 OR tbl_buyer_required_limit.termSheetSignedByBank != 1) 
      `
      if(reqBody.onlyShowForUserId){
        extraSearchQry += ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
      }
      if(reqBody.subadminIds){
        extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }

      filterQuery = `SELECT tbl_buyers_detail.id,
        supplierDetails.company_name AS supplierName,
        GROUP_CONCAT(tbl_user_details.company_name) AS selectedLenderName,
        (LENGTH(tbl_buyer_required_limit.buyers_credit) - LENGTH(REPLACE(tbl_buyer_required_limit.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
        COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender
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
        supplierDetails.tbl_user_id = tbl_user.id
        LEFT JOIN tbl_user_details adminDetails ON
        adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
        WHERE tbl_buyer_required_limit.buyerId IS NOT NULL AND tbl_buyer_required_limit.limitPendingFrom IS NULL
        ${extraSearchQry}
        GROUP BY tbl_share_invoice_quote_request.quoteId HAVING ${havingSearchQry} `;
        
      if(!reqBody.dontFetchCountStats){
        filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
        filterCount["expired"] = filterDbRes.message.length
      }

      res.send({success: true, message: {filterCount, listData: dbRes.message, countData: countDbRes.message.length}});
    }

    if(reqBody.atFinanceStage){

      let sortString = ` ORDER BY tbl_invoice_discounting.created_at DESC `
      let havingSearchQry = " HAVING " 
      let searchQuery = ""
      let perPageString = "";
      let extraSearchQry = "";

      if(reqBody.sortDateBy){
        sortString = ` ORDER BY tbl_invoice_discounting.created_at ${reqBody.sortDateBy} `
      }
      
      if(reqBody.sortExpName){
        sortString = ` ORDER BY supplierDetails.company_name ${reqBody.sortExpName} `
      }

      if(reqBody.sortBuyerName){
        sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortBuyerName} `
      }

      if(reqBody.search){
        searchQuery = ` AND (tbl_buyers_detail.buyerName LIKE '%${reqBody.search}%' OR supplierDetails.company_name LIKE '%${reqBody.search}%' 
        OR tbl_invoice_discounting.reference_no LIKE '%${reqBody.search}%' OR tbl_invoice_discounting.stenn_deal_id LIKE '%${reqBody.search}%' OR 
        tbl_invoice_discounting.modifi_deal_id LIKE '%${reqBody.search}%' ) `
      }

      if(reqBody.resultPerPage && reqBody.currentPage) {
        perPageString = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage}`;
      } 

      if(havingSearchQry === " HAVING "){
        havingSearchQry = ""
      }

      if(reqBody.buyerName){
        extraSearchQry += ` AND tbl_buyers_detail.buyerName IN (${reqBody.buyerName.join(",")}) `
      }

      if(reqBody.exporterName){
        extraSearchQry += ` AND supplierDetails.company_name IN (${reqBody.exporterName.join(",")}) `
      }

      if(reqBody.admins){
        extraSearchQry += ` AND adminDetails.contact_person IN (${reqBody.admins.join(",")}) `
      }

      if(reqBody.financiersFilter){
        extraSearchQry += ` AND lenderDetails.company_name IN (${reqBody.financiersFilter.join(",")}) `
      }

      if (reqBody.dateRangeFilter) {
        if (reqBody.dateRangeFilter[0] && !reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_invoice_discounting.created_at >= '${reqBody.dateRangeFilter[0]}' `
        }
        else if (!reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_invoice_discounting.created_at <= '${reqBody.dateRangeFilter[1]}'  `
        }
        else if (reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_invoice_discounting.created_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' `
        }
      }

      if(reqBody.onlyShowForUserId){
        extraSearchQry += ` AND ( tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}')`
      }
      if(reqBody.subadminIds){
        extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }

      if (reqBody.status) {
        let isApplied = reqBody.status.includes("Applied")
        let isInprogress = reqBody.status.includes("Inprogress")
        let isApproved = reqBody.status.includes("Approved")
        let isRejected = reqBody.status.includes("Rejected")
        let isDisbursed = reqBody.status.includes("Disbursed")
        let lastElement = reqBody.status[reqBody.status.length - 1]
        // 3 - approved
        // 5 - rejected
        // 4 - disbursed
        // 6 - In progress
        extraSearchQry += ` AND `
        if(isApplied){
          extraSearchQry += ` (tbl_invoice_discounting.status !=3 AND tbl_invoice_discounting.status !=4 AND 
          tbl_invoice_discounting.status !=5 AND tbl_invoice_discounting.status !=6) 
          ${lastElement === "Applied" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
        }
        // if(isInprogress){
        //   extraSearchQry += `  tbl_invoice_discounting.status = 6 
        //   ${lastElement === "Inprogress" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
        // }
        if(isApproved){
          extraSearchQry += ` (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6 )
          ${lastElement === "Approved" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
        }
        if(isRejected){
          extraSearchQry += ` tbl_invoice_discounting.status = 5 
          ${lastElement === "Rejected" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
        }
        // if(isDisbursed){
        //   extraSearchQry += ` tbl_invoice_discounting.status = 4
        //   ${lastElement === "Disbursed" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
        // }
      }

      let query = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id,
      tbl_buyers_detail.id, tbl_buyers_detail.identifier, tbl_buyers_detail.stenn_id, tbl_buyers_detail.user_id, 
      tbl_buyers_detail.buyerName, tbl_buyers_detail.buyerEmail,tbl_buyers_detail.buyerPhone,tbl_buyers_detail.buyerAddress,
      tbl_buyers_detail.buyerDUNSNo, tbl_buyers_detail.buyerWebsite, tbl_buyers_detail.buyerCurrency,
      tbl_buyers_detail.buyersDocId, tbl_buyers_detail.previousAnnualSale, tbl_buyers_detail.currentAnnualSale,
      tbl_buyers_detail.buyerCountry, tbl_buyers_detail.buyerPosition, tbl_buyers_detail.nameAuthorizedSignatory,
      tbl_buyers_detail.incoTerms, tbl_buyers_detail.productDetails,
      tbl_buyers_detail.termsOfPayment,
      tbl_buyers_detail.buyersAPIDetail,
      tbl_buyers_detail.buyerNameTitle,
      tbl_buyers_detail.lead_by,
      tbl_buyers_detail.buyerPhoneCode,
      tbl_buyers_detail.buyerHsnCode,
      tbl_buyers_detail.buyerOtherDocs,
      tbl_buyers_detail.ttvId, 
      tbl_buyers_detail.annualTurnOver,
      tbl_buyers_detail.isPromotedToEdit,
      tbl_buyers_detail.ActionBy,
      tbl_buyers_detail.created_by,
      tbl_buyer_required_limit.buyers_credit,
      tbl_buyer_required_limit.buyersRemark,

      tbl_buyer_required_limit.id AS applicationId, tbl_buyer_required_limit.expShipmentDate,
      tbl_invoice_discounting.reference_no AS invRefNo,
      tbl_invoice_discounting.reference_no,
      tbl_invoice_discounting.stenn_deal_id,
      tbl_invoice_discounting.created_at AS invoiceApplicationCreatedAt,
      tbl_invoice_discounting.status AS invoiceStatus,
      tbl_buyer_required_limit.updatedAt AS applicationUpdatedAt,
      tbl_buyer_required_limit.createdAt AS applicationCreatedAt,   
      tbl_buyer_required_limit.termSheet, 
      tbl_buyer_required_limit.termSheetSignedByExporter,
      tbl_buyer_required_limit.termSheetSignedByBank,  
      tbl_buyer_required_limit.documentStatus,
      tbl_buyer_required_limit.selectedQuote,
      tbl_buyer_required_limit.frameworkDoc,
      tbl_buyer_required_limit.exhibitDoc,
      tbl_buyer_required_limit.noaDoc,
      tbl_buyer_required_limit.selectedQuote,

      frameworkSignStatus.signatureId AS frameworkExporterSign,
      frameworkSignStatus.financierSignatureId AS frameworkFinancierSign,
      frameworkSignStatus.buyerSignatureId AS frameworkBuyerSign,

      exhibitSignStatus.signatureId AS exhibitExporterSign,
      exhibitSignStatus.financierSignatureId AS exhibitFinancierSign,
      exhibitSignStatus.buyerSignatureId AS exhibitBuyerSign,

      noaSignStatus.signatureId AS noaExporterSign,
      noaSignStatus.financierSignatureId noaFinancierSign,
      noaSignStatus.buyerSignatureId noaBuyerSign,
      tbl_countries.name AS supplierCountryName,

      supplierDetails.company_name AS supplierName,
      lenderDetails.company_name AS lenderName,
      adminDetails.contact_person AS leadAssignToName,
      adminDetails.tbl_user_id AS leadAssignToId
      FROM tbl_invoice_discounting 
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id

      LEFT JOIN tbl_document_details frameworkSignStatus ON
      tbl_buyer_required_limit.frameworkDoc = frameworkSignStatus.id

      LEFT JOIN tbl_document_details exhibitSignStatus ON
      tbl_buyer_required_limit.exhibitDoc = exhibitSignStatus.id

      LEFT JOIN tbl_document_details noaSignStatus ON
      tbl_buyer_required_limit.noaDoc = noaSignStatus.id

      LEFT JOIN tbl_countries ON
      supplierDetails.country_code = tbl_countries.sortname

      WHERE 1 
      ${searchQuery} ${extraSearchQry} ${havingSearchQry}
      ${sortString} ${perPageString}` ;

      console.log('query atFinanceStage =========================>', query);

      let countQuery = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
      FROM tbl_invoice_discounting 
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE 1 
      ${searchQuery} ${extraSearchQry} ${havingSearchQry}
      ${sortString}` ;

      let dbRes = await call({ query }, 'makeQuery', 'get');
      let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');
      if(reqBody.onlyShowForUserId){
        extraSearchQry = ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
      }
      if(reqBody.subadminIds){
        extraSearchQry = ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }
      let filterCount = {}
      // Applied
      let filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
      FROM tbl_invoice_discounting 
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE tbl_invoice_discounting.status != 3 AND tbl_invoice_discounting.status != 4 AND tbl_invoice_discounting.status != 5 AND 
      tbl_invoice_discounting.status != 6 ${extraSearchQry}`
      filterCount["applied"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      filterCount["applied"] = filterCount["applied"].message.length; 
      // Approved
      filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
      FROM tbl_invoice_discounting 
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) ${extraSearchQry}`
      filterCount["approved"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      filterCount["approved"] = filterCount["approved"].message.length; 
      // Rejected
      filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
      FROM tbl_invoice_discounting 
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE tbl_invoice_discounting.status = 5 ${extraSearchQry}`
      filterCount["rejected"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      filterCount["rejected"] = filterCount["rejected"].message.length; 
      // Inprogress
      // filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
      // FROM tbl_invoice_discounting 
      // LEFT JOIN tbl_buyer_required_limit ON
      // tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      // LEFT JOIN tbl_buyers_detail ON
      // tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      // LEFT JOIN tbl_user_details supplierDetails ON
      // tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      // LEFT JOIN tbl_user_details lenderDetails ON
      // tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      // WHERE tbl_invoice_discounting.status = 6 `
      // filterCount["inprogress"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      // filterCount["inprogress"] = filterCount["inprogress"].message.length; 
      // Disbursed
      // filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
      // FROM tbl_invoice_discounting 
      // LEFT JOIN tbl_buyer_required_limit ON
      // tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      // LEFT JOIN tbl_buyers_detail ON
      // tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      // LEFT JOIN tbl_user_details supplierDetails ON
      // tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      // LEFT JOIN tbl_user_details lenderDetails ON
      // tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      // WHERE tbl_invoice_discounting.status = 4 `
      // filterCount["disbursed"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      // filterCount["disbursed"] = filterCount["disbursed"].message.length; 

      res.send({success: true, message: { filterCount, listData: dbRes.message, countData: countDbRes.message.length}});
    }

    if(reqBody.atApprovedStage){
      let sortString = ` ORDER BY tbl_invoice_discounting.created_at DESC `
      let havingSearchQry = " HAVING " 
      let searchQuery = ""
      let perPageString = "";
      let extraSearchQry = "";

      if(reqBody.resultPerPage && reqBody.currentPage) {
        perPageString = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage}`;
      }

      if(reqBody.search){
        searchQuery = ` AND (tbl_buyers_detail.buyerName LIKE '%${reqBody.search}%' OR supplierDetails.company_name LIKE '%${reqBody.search}%' 
        OR tbl_invoice_discounting.reference_no LIKE '%${reqBody.search}%' OR tbl_invoice_discounting.stenn_deal_id LIKE '%${reqBody.search}%' OR 
        tbl_invoice_discounting.modifi_deal_id LIKE '%${reqBody.search}%' ) `
      }

      if(reqBody.sortExpName){
        sortString = ` ORDER BY supplierDetails.company_name ${reqBody.sortExpName} `
      }

      if(reqBody.sortBuyerName){
        sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortBuyerName} `
      }

      if(reqBody.sortFinancier){
        sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortFinancier} `
      }
      if(reqBody.sortTermsofPayment){
        sortString = ` ORDER BY tbl_buyers_detail.termsOfPayment ${reqBody.sortTermsofPayment} `
      }
      if(reqBody.sortdisbursementdate){
        sortString = ` ORDER BY GROUP_CONCAT(tbl_disbursement_scheduled.scheduledOn ORDER BY tbl_disbursement_scheduled.id) ${reqBody.sortdisbursementdate} `
      }
      if(reqBody.sortdisburseAmount){
        sortString = ` ORDER BY GROUP_CONCAT(tbl_disbursement_scheduled.amount ORDER BY tbl_disbursement_scheduled.id)  ${reqBody.sortdisburseAmount} `
      }
      if(reqBody.sortOutStandingAmt){
        sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortOutStandingAmt} `
      }
      if(reqBody.sortOutStandingDays){
        sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortOutStandingDays} `
      }

      if(reqBody.sortApplicationNo){
        sortString = ` ORDER BY tbl_buyer_required_limit.invRefNo ${reqBody.sortApplicationNo} `
      }










      if(havingSearchQry === " HAVING "){
        havingSearchQry = ""
      }

      if(reqBody.buyerName){
        extraSearchQry += ` AND tbl_buyers_detail.buyerName IN (${reqBody.buyerName.join(",")}) `
      }

      if(reqBody.exporterName){
        extraSearchQry += ` AND supplierDetails.company_name IN (${reqBody.exporterName.join(",")}) `
      }
      
      if(reqBody.admins){
        extraSearchQry += ` AND adminDetails.contact_person IN (${reqBody.admins.join(",")}) `
      }

      if(reqBody.supplierId){
        extraSearchQry += ` AND supplierDetails.tbl_user_id = ${reqBody.supplierId} `
      }

      if(reqBody.financiersFilter){
        extraSearchQry += ` AND lenderDetails.company_name IN (${reqBody.financiersFilter.join(",")}) `
      }

      if (reqBody.dateRangeFilter) {
        if (reqBody.dateRangeFilter[0] && !reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_invoice_discounting.created_at >= '${reqBody.dateRangeFilter[0]}' `
        }
        else if (!reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_invoice_discounting.created_at <= '${reqBody.dateRangeFilter[1]}'  `
        }
        else if (reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_invoice_discounting.created_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' `
        }
      }
      if (reqBody.disbursementDateRangeFilter) {
        if (reqBody.disbursementDateRangeFilter[0] && !reqBody.disbursementDateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_disbursement_scheduled.scheduledOn >= '${reqBody.disbursementDateRangeFilter[0]}' `
        }
        else if (!reqBody.disbursementDateRangeFilter[0] && reqBody.disbursementDateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_disbursement_scheduled.scheduledOn <= '${reqBody.disbursementDateRangeFilter[1]}'  `
        }
        else if (reqBody.disbursementDateRangeFilter[0] && reqBody.disbursementDateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_disbursement_scheduled.scheduledOn BETWEEN '${reqBody.disbursementDateRangeFilter[0]}' AND '${reqBody.disbursementDateRangeFilter[1]}' `
        }
      }
      if(reqBody.onlyShowForUserId){
        extraSearchQry += ` AND ( tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}')`
      }
      if(reqBody.subadminIds){
        extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }

      let query = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id,
      tbl_buyers_detail.id, tbl_buyers_detail.identifier, tbl_buyers_detail.stenn_id, tbl_buyers_detail.user_id, 
      tbl_buyers_detail.buyerName, tbl_buyers_detail.buyerEmail,tbl_buyers_detail.buyerPhone,tbl_buyers_detail.buyerAddress,
      tbl_buyers_detail.buyerDUNSNo, tbl_buyers_detail.buyerWebsite, tbl_buyers_detail.buyerCurrency,
      tbl_buyers_detail.buyersDocId, tbl_buyers_detail.previousAnnualSale, tbl_buyers_detail.currentAnnualSale,
      tbl_buyers_detail.buyerCountry, tbl_buyers_detail.buyerPosition, tbl_buyers_detail.nameAuthorizedSignatory,
      tbl_buyers_detail.incoTerms, tbl_buyers_detail.productDetails,
      tbl_buyers_detail.termsOfPayment,
      tbl_buyers_detail.buyersAPIDetail,
      tbl_buyers_detail.buyerNameTitle,
      tbl_buyers_detail.lead_by,
      tbl_buyers_detail.buyerPhoneCode,
      tbl_buyers_detail.buyerHsnCode,
      tbl_buyers_detail.buyerOtherDocs,
      tbl_buyers_detail.ttvId, 
      tbl_buyers_detail.annualTurnOver,
      tbl_buyers_detail.isPromotedToEdit,
      tbl_buyers_detail.ActionBy,
      tbl_buyers_detail.created_by,
      tbl_buyer_required_limit.buyers_credit,
      tbl_buyer_required_limit.buyersRemark,
      adminDetails.contact_person AS leadAssignToName,
      adminDetails.tbl_user_id AS leadAssignToId,
      tbl_buyer_required_limit.id AS applicationId, tbl_buyer_required_limit.expShipmentDate,
      tbl_buyer_required_limit.invRefNo,
      tbl_invoice_discounting.reference_no,
      tbl_invoice_discounting.stenn_deal_id,
      tbl_invoice_discounting.created_at AS invoiceApplicationCreatedAt,
      tbl_invoice_discounting.status AS invoiceStatus,
      tbl_invoice_discounting.contractAmountInUSD,
      tbl_invoice_discounting.credit_days,
      tbl_buyer_required_limit.updatedAt AS applicationUpdatedAt,
      tbl_buyer_required_limit.createdAt AS applicationCreatedAt,   
      tbl_buyer_required_limit.termSheet, 
      tbl_buyer_required_limit.termSheetSignedByExporter,
      tbl_buyer_required_limit.termSheetSignedByBank,  
      tbl_buyer_required_limit.documentStatus,
      tbl_buyer_required_limit.selectedQuote,
      tbl_buyer_required_limit.frameworkDoc,
      tbl_buyer_required_limit.exhibitDoc,
      tbl_buyer_required_limit.noaDoc,
      tbl_buyer_required_limit.selectedQuote,

      frameworkSignStatus.signatureId AS frameworkExporterSign,
      frameworkSignStatus.financierSignatureId AS frameworkFinancierSign,
      frameworkSignStatus.buyerSignatureId AS frameworkBuyerSign,

      exhibitSignStatus.signatureId AS exhibitExporterSign,
      exhibitSignStatus.financierSignatureId AS exhibitFinancierSign,
      exhibitSignStatus.buyerSignatureId AS exhibitBuyerSign,

      noaSignStatus.signatureId AS noaExporterSign,
      noaSignStatus.financierSignatureId noaFinancierSign,
      noaSignStatus.buyerSignatureId noaBuyerSign,

      supplierDetails.company_name AS supplierName,
      lenderDetails.company_name AS lenderName,
      GROUP_CONCAT(tbl_disbursement_scheduled.scheduledOn ORDER BY tbl_disbursement_scheduled.id ASC) AS disbScheduledOn,
      GROUP_CONCAT(tbl_disbursement_scheduled.amount ORDER BY tbl_disbursement_scheduled.id ASC) AS disbAmount,
      GROUP_CONCAT(tbl_disbursement_scheduled.currency ORDER BY tbl_disbursement_scheduled.id ASC) AS disbCurrency,
      GROUP_CONCAT(tbl_disbursement_scheduled.status ORDER BY tbl_disbursement_scheduled.id ASC) AS disbStatus,
      GROUP_CONCAT(IFNULL(tbl_disbursement_scheduled.disbursedAmount, 'NA') ORDER BY tbl_disbursement_scheduled.id ASC) AS disbActualAmount,
      GROUP_CONCAT(IFNULL(tbl_disbursement_scheduled.updatedAt, 'NA') ORDER BY tbl_disbursement_scheduled.id ASC) AS disbActualDate,
      GROUP_CONCAT(IFNULL(tbl_disbursement_scheduled.attachment, 'NA') ORDER BY tbl_disbursement_scheduled.id ASC) AS disbAttachment,

      (
        SELECT GROUP_CONCAT(chat_room_id SEPARATOR ',')
        FROM tbl_chat_rooms
        WHERE tbl_chat_rooms.invApplicationId = tbl_buyer_required_limit.id
      ) AS chatRoomIds,
      (
        SELECT GROUP_CONCAT(included_users SEPARATOR ',')
        FROM tbl_chat_rooms
        WHERE tbl_chat_rooms.invApplicationId = tbl_buyer_required_limit.id
      ) AS chatRoomUsers,
      (
        SELECT GROUP_CONCAT(
            COALESCE(tbl_chatroom_unread_msg.count, '0')
            ORDER BY tbl_chat_rooms.chat_room_id
            SEPARATOR ','
        )
        FROM tbl_chat_rooms
        LEFT JOIN tbl_chatroom_unread_msg ON tbl_chat_rooms.chat_room_id = tbl_chatroom_unread_msg.chatRoomId
            AND tbl_chatroom_unread_msg.userId = '${reqBody.supplierId || reqBody.userId}'
        WHERE tbl_chat_rooms.invApplicationId = tbl_buyer_required_limit.id
      ) AS chatRoomUnreadMsgCount,
      tbl_invoice_discounting.created_at AS invoiceApplicationDate
      FROM tbl_invoice_discounting 

      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id

      LEFT JOIN tbl_document_details frameworkSignStatus ON
      tbl_buyer_required_limit.frameworkDoc = frameworkSignStatus.id

      LEFT JOIN tbl_document_details exhibitSignStatus ON
      tbl_buyer_required_limit.exhibitDoc = exhibitSignStatus.id

      LEFT JOIN tbl_document_details noaSignStatus ON
      tbl_buyer_required_limit.noaDoc = noaSignStatus.id

      LEFT JOIN tbl_disbursement_scheduled ON
      tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo

      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6)
      ${searchQuery} ${extraSearchQry}
      GROUP BY tbl_invoice_discounting.reference_no
      ${havingSearchQry}
      ${sortString} ${perPageString}` ;

      console.log('query atApprovedStage =========================>', query);

      let countQuery = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
      FROM tbl_invoice_discounting 
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      LEFT JOIN tbl_disbursement_scheduled ON
      tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) 
      ${searchQuery} ${extraSearchQry}
      GROUP BY tbl_invoice_discounting.reference_no ${havingSearchQry}
      ${sortString}` ;

      let dbRes = await call({ query }, 'makeQuery', 'get');
      let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');

      let filterCount = {}

      res.send({success: true, message: { filterCount, listData: dbRes.message, countData: countDbRes.message.length}});
    }
  }
  catch (error) {
    console.log("in getInvoiceDiscountingListForAdmin error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getInvoiceDiscountingFiltersForAdmin = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filterData = {}
    //
    filterData["Date"] = {
      "accordianId": 'dateRangeFilter',
      type: "minMaxDate",
      value: []
    }
    //
    let query = ''
    
    if(reqBody.atLimitStage){
      query = `SELECT DISTINCT tbl_user_details.company_name AS name FROM tbl_user_details
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_user_details.tbl_user_id = tbl_buyer_required_limit.userId
      WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
      ORDER BY tbl_user_details.company_name ASC `
    }
    if(reqBody.atFinanceStage){
      query = `SELECT DISTINCT tbl_user_details.company_name AS name FROM tbl_invoice_discounting
      LEFT JOIN tbl_user_details ON
      tbl_invoice_discounting.seller_id = tbl_user_details.tbl_user_id
      ORDER BY tbl_user_details.company_name ASC `
    }
    if(reqBody.atApprovedStage){
      query = `SELECT DISTINCT tbl_user_details.company_name AS name FROM tbl_invoice_discounting
      LEFT JOIN tbl_user_details ON
      tbl_invoice_discounting.seller_id = tbl_user_details.tbl_user_id
      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6)
      ORDER BY tbl_user_details.company_name ASC `
    }
    
    let dbRes = await call({query}, 'makeQuery', 'get');
    filterData["Exporter Name"] = {
      "accordianId": 'exporterName',
      type: "checkbox",
      data: dbRes.message,
      labelName: "name"
    } 
    //
    if(reqBody.atLimitStage){
      query = `SELECT DISTINCT tbl_buyers_detail.buyerName AS name FROM tbl_buyers_detail
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_buyers_detail.id = tbl_buyer_required_limit.buyerId
      WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
      ORDER BY tbl_buyers_detail.buyerName ASC `
    }
    if(reqBody.atFinanceStage){
      query = `SELECT DISTINCT tbl_buyers_detail.buyerName AS name FROM tbl_invoice_discounting
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      ORDER BY tbl_buyers_detail.buyerName ASC `
    }
    if(reqBody.atApprovedStage){
      filterData["Disbursement Date"] = {
        "accordianId": 'disbursementDateRangeFilter',
        type: "minMaxDate",
        value: []
      }
      filterData["Application Date"] =  {
        "accordianId": 'dateRangeFilter',
        type: "minMaxDate",
        value: []
      }
      delete filterData["Date"]
      let supplierIdFilterQuery = ''
      if(reqBody.supplierId){
        supplierIdFilterQuery = `  AND tbl_invoice_discounting.seller_id = '${reqBody.supplierId}'  `
      }      
      query = `SELECT DISTINCT tbl_buyers_detail.buyerName AS name FROM tbl_invoice_discounting
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6)
      ${supplierIdFilterQuery} ORDER BY tbl_buyers_detail.buyerName ASC `
    }
    dbRes = await call({query}, 'makeQuery', 'get');
    filterData["Buyer Name"] = {
      "accordianId": 'buyerName',
      type: "checkbox",
      data: dbRes.message,
      labelName: "name"
    }   
    //4
    filterData["Financier Selected"] = {
      "accordianId": 'financiersFilter',
      type: "checkbox",
      data: await emailEnabledBanks(),
      labelName: "name"
    }
    //
    filterData["Status"] = {
      "accordianId": 'status',
      type: "checkbox",
      labelName: "name"
    }
    if(reqBody.atLimitStage){
      filterData["Status"]["data"] = [{name: "Under Review"},{name: "Inprogress"},{name: "Approved"}, {name: 'Rejected'}, {name: "Expired"}]
    }
    if(reqBody.atFinanceStage){
      // filterData["Status"]["data"] = [{name: "Applied"},{name: "Inprogress"}, {name: 'Approved'}, {name: 'Rejected'},
      // {name: 'Disbursed'}]
      filterData["Status"]["data"] = [{name: "Applied"},{name: 'Approved'}, {name: 'Rejected'}]
    }
    if(reqBody.atApprovedStage){
      delete filterData["Status"]
    }
    // if(reqBody.atLimitStage){
    query = `SELECT tbl_user_details.contact_person AS name FROM tbl_user_details LEFT JOIN tbl_user ON tbl_user.id = tbl_user_details.tbl_user_id
      WHERE tbl_user.type_id = 1 `
    dbRes = await call({ query }, 'makeQuery', 'get');
    filterData["Admins"] = {
      "accordianId": 'admins',
      type: "checkbox",
      data: dbRes.message,
      labelName: "name"
    }
    // }
    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("in getInvoiceDiscountingFiltersForAdmin error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getTransactionsHistoryForShipmentBooking = async (req, res, next) => {
  try {
    let {contractNo} = req.body
    let transactionHistory = []

    let query = formatSqlQuery(`SELECT * FROM tbl_shipment_booking_application WHERE 
    JSON_EXTRACT(tbl_shipment_booking_application.details, "$.commodityContract") = ? `,[contractNo])
    let dbRes = await call({ query }, 'makeQuery', 'get');

    if(dbRes.message?.[0]){
      let temp = dbRes.message?.[0]      
      transactionHistory.push({
        action: `Applied for Logistic Quote`,
        dateTime: temp.createdAt,
        date: temp.createdAt ? moment(temp.createdAt).format("DD MMM, YYYY") : "NA",
        time: temp.createdAt ? moment(temp.createdAt).format("hh:mm a") : "NA"
      })
    }
    if(dbRes.message?.[0]?.shipperQuotes){
      let shipperQuotes = JSON.parse(dbRes.message?.[0]?.shipperQuotes)
      for (let index = 0; index < shipperQuotes.length; index++) {
        let element = shipperQuotes[index]
        let shipperQry = `SELECT * FROM tbl_user_details WHERE tbl_user_id = ${element['shipper_id']} `
        let shipperDbRes = await call({ query: shipperQry}, 'makeQuery', 'get');

        transactionHistory.push({
          action: element['shipperAction']==="Approved" ? `Quote approved by ${shipperDbRes.message[0]?.company_name}` : `Quote denied by ${shipperDbRes.message[0]?.company_name}`,
          dateTime: element.assignDate,
          date: element.assignDate ? moment(element.assignDate).format("DD MMM, YYYY") : "NA",
          time: element.assignDate ? moment(element.assignDate).format("hh:mm a") : "NA"
        })                
      }
    }
    if(dbRes.message?.[0]?.selectedShipper){
      let shipperQry = `SELECT * FROM tbl_user_details WHERE tbl_user_id = ${dbRes.message?.[0]?.selectedShipper} `
      let shipperDbRes = await call({ query: shipperQry}, 'makeQuery', 'get');    
      transactionHistory.push({
        action: `Shipment booked with ${shipperDbRes.message[0]?.company_name}`,
        dateTime: dbRes.message?.[0]?.shipperSelectedOn,
        date: dbRes.message?.[0]?.shipperSelectedOn ? moment(dbRes.message?.[0]?.shipperSelectedOn).format("DD MMM, YYYY") : "NA",
        time: dbRes.message?.[0]?.shipperSelectedOn ? moment(dbRes.message?.[0]?.shipperSelectedOn).format("hh:mm a") : "NA"
      })
    }
    res.send({success: true, message: transactionHistory})
  }
  catch (error) {
    console.log("in getTransactionsHistoryForShipmentBooking error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getTransactionHistoryForInvoiceLimit = async (req, res, next) => {
  try {
    let reqBody = req.body
    let transactionHistory = []

    let query = `SELECT buyers_credit FROM tbl_buyer_required_limit WHERE id = '${reqBody.applicationId}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');

    query = `SELECT * FROM tbl_buyer_required_limit WHERE id = '${reqBody.applicationId}' `
    let limitDbResp = await call({ query }, 'makeQuery', 'get');

    // limit created on
    if(limitDbResp.message?.[0]){
      let temp = limitDbResp.message?.[0]
      transactionHistory.push({
        action: `Applied for finance limit`,
        dateTime: temp.createdAt,
        date: temp.createdAt ? moment(temp.createdAt).format("DD MMM, YYYY") : "NA",
        time: temp.createdAt ? moment(temp.createdAt).format("hh:mm a") : "NA"
      })
    }

    if(dbRes.message?.[0]?.buyers_credit){
      // quote details
      let temp = JSON.parse(dbRes.message?.[0]?.buyers_credit) || []
      for (let index = 0; index < temp.length; index++) {
        const element = temp[index];
        transactionHistory.push({
          action: element.financierAction === "deny" ? `Quote denied by ${element.lender_name}` : `Quote approved by ${element.lender_name}`,
          dateTime: element.assignDate,
          date: element.assignDate ? moment(element.assignDate).format("DD MMM, YYYY") : "NA",
          time: element.assignDate ? moment(element.assignDate).format("hh:mm a") : "NA"
        })
      }
      // quote selected on
      if(limitDbResp.message?.[0]?.selectedQuote){
        let temp = limitDbResp.message?.[0]
        transactionHistory.push({
          action: `Quote from ${temp.selectedQuote.lender_name} selected by exporter`,
          dateTime: temp.quoteSelectedOn,
          date: temp.quoteSelectedOn ? moment(temp.quoteSelectedOn).format("DD MMM, YYYY") : "NA",
          time: temp.quoteSelectedOn ? moment(temp.quoteSelectedOn).format("hh:mm a") : "NA"
        })
      }
      // term sheet sent on
      if(limitDbResp.message?.[0]?.termSheet!=null){
        let temp = limitDbResp.message?.[0]
        transactionHistory.push({
          action: `Term sheet sent by ${temp.selectedQuote.lender_name} `,
          dateTime: temp.termSheetSentOn,
          date: temp.termSheetSentOn ? moment(temp.termSheetSentOn).format("DD MMM, YYYY") : "NA",
          time: temp.termSheetSentOn ? moment(temp.termSheetSentOn).format("hh:mm a") : "NA"
        })
      }
      // term sheet signed by financier
      if(limitDbResp.message?.[0]?.termSheetSignedByBank){
        let temp = limitDbResp.message?.[0]
        transactionHistory.push({
          action: `Term sheet signed by ${temp.selectedQuote.lender_name} financier`,
          dateTime: temp.termSheetSignedByFinancerOn,
          date: temp.termSheetSignedByFinancerOn ? moment(temp.termSheetSignedByFinancerOn).format("DD MMM, YYYY") : "NA",
          time: temp.termSheetSignedByFinancerOn ? moment(temp.termSheetSignedByFinancerOn).format("hh:mm a") : "NA"
        })
      }
      // term sheet signed by supplier
      if(limitDbResp.message?.[0]?.termSheetSignedByExporter){
        let temp = limitDbResp.message?.[0]
        transactionHistory.push({
          action: `Term sheet signed by exporter `,
          dateTime: temp.termSheetSignedByExporterOn,
          date: temp.termSheetSignedByExporterOn ? moment(temp.termSheetSignedByExporterOn).format("DD MMM, YYYY") : "NA",
          time: temp.termSheetSignedByExporterOn ? moment(temp.termSheetSignedByExporterOn).format("hh:mm a") : "NA"
        })
      }
    } 

    // For atFinance Stage start
    if(reqBody.invRefNo){
      // 3 - approved
      // 5 - rejected
      // 4 - disbursed
      // 6 - In progress
      query = `SELECT tbl_invoice_discounting.status, 
      tbl_invoice_discounting.created_at,
      tbl_invoice_discounting_audit_logs.*,

      tbl_buyer_required_limit.frameworkDoc,
      tbl_buyer_required_limit.exhibitDoc,
      tbl_buyer_required_limit.noaDoc,

      frameworkSignStatus.signatureId AS frameworkExporterSign,
      frameworkSignStatus.financierSignatureId AS frameworkFinancierSign,
      frameworkSignStatus.buyerSignatureId AS frameworkBuyerSign,

      exhibitSignStatus.signatureId AS exhibitExporterSign,
      exhibitSignStatus.financierSignatureId AS exhibitFinancierSign,
      exhibitSignStatus.buyerSignatureId AS exhibitBuyerSign,

      noaSignStatus.signatureId AS noaExporterSign,
      noaSignStatus.financierSignatureId noaFinancierSign,
      noaSignStatus.buyerSignatureId noaBuyerSign

      FROM tbl_invoice_discounting 

      LEFT JOIN tbl_invoice_discounting_audit_logs ON
      tbl_invoice_discounting.reference_no = tbl_invoice_discounting_audit_logs.invRefNo

      LEFT JOIN tbl_buyer_required_limit ON
      tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo

      LEFT JOIN tbl_document_details frameworkSignStatus ON
      tbl_buyer_required_limit.frameworkDoc = frameworkSignStatus.id

      LEFT JOIN tbl_document_details exhibitSignStatus ON
      tbl_buyer_required_limit.exhibitDoc = exhibitSignStatus.id

      LEFT JOIN tbl_document_details noaSignStatus ON
      tbl_buyer_required_limit.noaDoc = noaSignStatus.id

      WHERE tbl_invoice_discounting.reference_no = '${reqBody.invRefNo}' `

      dbRes = await call({ query }, 'makeQuery', 'get');
      let invoiceData = dbRes.message?.[0] || {}
      // invoice created at
      transactionHistory.push({
        action: `Invoice Application Applied `,
        dateTime: invoiceData.created_at,
        date: invoiceData.created_at ? moment(invoiceData.created_at).format("DD MMM, YYYY") : "NA",
        time: invoiceData.created_at ? moment(invoiceData.created_at).format("hh:mm a") : "NA"
      })
      if(invoiceData?.status/1 == 5){
        // invoice rejected at
        transactionHistory.push({
        action: `Invoice Application Rejected `,
        dateTime: invoiceData.rejectedOn,
        date: invoiceData.rejectedOn ? moment(invoiceData.rejectedOn).format("DD MMM, YYYY") : "NA",
        time: invoiceData.rejectedOn ? moment(invoiceData.rejectedOn).format("hh:mm a") : "NA"
        }) 
      }
      // adding invoice agreement statuses start
      else{
        // agreement sent log
        if(invoiceData?.frameworkDoc || invoiceData?.exhibitDoc || invoiceData?.noaDoc){
          transactionHistory.push({
            action: `Agreement Sent By Financier  `,
            dateTime: invoiceData.agreementSentOn,
            date: invoiceData.agreementSentOn ? moment(invoiceData.agreementSentOn).format("DD MMM, YYYY") : "NA",
            time: invoiceData.agreementSentOn ? moment(invoiceData.agreementSentOn).format("hh:mm a") : "NA"
            }) 
        }   
        // agreement sign by exporter log
        if(invoiceData?.frameworkExporterSign || invoiceData?.exhibitExporterSign || invoiceData?.noaExporterSign){
          transactionHistory.push({
            action: `Agreement Signed By Exporter `,
            dateTime: invoiceData.agreementSignByExporter,
            date: invoiceData.agreementSignByExporter ? moment(invoiceData.agreementSignByExporter).format("DD MMM, YYYY") : "NA",
            time: invoiceData.agreementSignByExporter ? moment(invoiceData.agreementSignByExporter).format("hh:mm a") : "NA"
            }) 
        } 
        // agreement sign by buyer log
        if(invoiceData?.frameworkBuyerSign || invoiceData?.exhibitBuyerSign || invoiceData?.noaBuyerSign){
          transactionHistory.push({
            action: `Agreement Signed By Buyer `,
            dateTime: invoiceData.agreementSignByBuyer,
            date: invoiceData.agreementSignByBuyer ? moment(invoiceData.agreementSignByBuyer).format("DD MMM, YYYY") : "NA",
            time: invoiceData.agreementSignByBuyer ? moment(invoiceData.agreementSignByBuyer).format("hh:mm a") : "NA"
            }) 
        } 
        // agreement sign by financier log
        if(invoiceData?.frameworkFinancierSign || invoiceData?.exhibitFinancierSign || invoiceData?.noaFinancierSign){
          transactionHistory.push({
            action: `Agreement Signed By Financier `,
            dateTime: invoiceData.agreementSignByFinancier,
            date: invoiceData.agreementSignByFinancier ? moment(invoiceData.agreementSignByFinancier).format("DD MMM, YYYY") : "NA",
            time: invoiceData.agreementSignByFinancier ? moment(invoiceData.agreementSignByFinancier).format("hh:mm a") : "NA"
            }) 
        }
      }
      // adding invoice agreement statuses end
      if(invoiceData?.status/1 == 3 || invoiceData?.status/1 == 4 || invoiceData?.status/1 == 6){
        // invoice approved at
        transactionHistory.push({
        action: `Invoice Application Approved `,
        dateTime: invoiceData.approvedOn,
        date: invoiceData.approvedOn ? moment(invoiceData.approvedOn).format("DD MMM, YYYY") : "NA",
        time: invoiceData.approvedOn ? moment(invoiceData.approvedOn).format("hh:mm a") : "NA"
        }) 
        if(invoiceData?.status/1 == 4 || invoiceData?.status/1 == 6){
          // invoice inprogress at
          transactionHistory.push({
            action: `Invoice Application Disbursement Inprogress `,
            dateTime: invoiceData.inprogressOn,
            date: invoiceData.inprogressOn ? moment(invoiceData.inprogressOn).format("DD MMM, YYYY") : "NA",
            time: invoiceData.inprogressOn ? moment(invoiceData.inprogressOn).format("hh:mm a") : "NA"
            })
          if(invoiceData?.status/1 == 4){
            // invoice disbursed at
            transactionHistory.push({
            action: `Invoice Application Disbursed `,
            dateTime: invoiceData.disbursedOn,
            date: invoiceData.disbursedOn ? moment(invoiceData.disbursedOn).format("DD MMM, YYYY") : "NA",
            time: invoiceData.disbursedOn ? moment(invoiceData.disbursedOn).format("hh:mm a") : "NA"
            })
          }
        }
      }
    }
    // For atFinance Stage end

    // Sorting the audit log by datetime
    if(transactionHistory.length){
      transactionHistory.sort(function(a, b) {
        return a.dateTime - b.dateTime;
      });
      transactionHistory.reverse()
    }
    res.send({
      success: true,
      message: transactionHistory
    })
  }
  catch (error) {
    console.log("in getTransactionHistoryForInvoiceLimit error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}


exports.getDisbursementStatsSummary = async (req, res, next) => {
  try {
    let reqBody = req.body
    let response  = {}
    let todaysDate = moment().format("YYYY-MM-DD")
    let extraSearchQry = ''
    if(reqBody.onlyShowForUserId){
      extraSearchQry += ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
    }
    if(reqBody.subadminIds){
      extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
    }
    // totalDisbursed 
    let query = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDisbursed FROM tbl_disbursement_scheduled
    LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.invRefNo = tbl_disbursement_scheduled.invRefNo
    LEFT JOIN tbl_user ON 
    tbl_user.id = tbl_buyer_required_limit.userId
    LEFT JOIN tbl_user_details adminDetails ON
    adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
    WHERE tbl_disbursement_scheduled.status = 1 ${extraSearchQry}`
    let dbRes = await call({ query }, 'makeQuery', 'get');
    response["totalDisbursed"] = dbRes.message?.[0]?.["totalDisbursed"] || 0
    // totalOverDue 
    query = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalOverDue FROM tbl_disbursement_scheduled
    LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.invRefNo = tbl_disbursement_scheduled.invRefNo
    LEFT JOIN tbl_user ON 
    tbl_user.id = tbl_buyer_required_limit.userId
    LEFT JOIN tbl_user_details adminDetails ON
    adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
    WHERE tbl_disbursement_scheduled.status = 0 AND scheduledOn < '${todaysDate}' ${extraSearchQry}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["totalOverDue"] = dbRes.message?.[0]?.["totalOverDue"] || 0

    // 12 week summary
    // totalDisbursed12Week
    let dateBefore12Week = moment().subtract(12, 'weeks').format('YYYY-MM-DD')
    query = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDisbursed12Week FROM tbl_disbursement_scheduled
    LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.invRefNo = tbl_disbursement_scheduled.invRefNo
    LEFT JOIN tbl_user ON 
    tbl_user.id = tbl_buyer_required_limit.userId
    LEFT JOIN tbl_user_details adminDetails ON
    adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
    WHERE tbl_disbursement_scheduled.status = 1 AND scheduledOn BETWEEN '${dateBefore12Week}' AND '${todaysDate}' ${extraSearchQry}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["totalDisbursed12Week"] = dbRes.message?.[0]?.["totalDisbursed12Week"] || 0
    // totalDue12Week
    query = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDue12Week FROM tbl_disbursement_scheduled
    LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.invRefNo = tbl_disbursement_scheduled.invRefNo
    LEFT JOIN tbl_user ON 
    tbl_user.id = tbl_buyer_required_limit.userId
    LEFT JOIN tbl_user_details adminDetails ON
    adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
    WHERE tbl_disbursement_scheduled.status = 0 AND scheduledOn > '${dateBefore12Week}' ${extraSearchQry}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["totalDue12Week"] = dbRes.message?.[0]?.["totalDue12Week"] || 0
    // totalOverDue12Week
    query = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalOverDue12Week FROM tbl_disbursement_scheduled
    LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.invRefNo = tbl_disbursement_scheduled.invRefNo
    LEFT JOIN tbl_user ON 
    tbl_user.id = tbl_buyer_required_limit.userId
    LEFT JOIN tbl_user_details adminDetails ON
    adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
    WHERE tbl_disbursement_scheduled.status = 0 AND scheduledOn BETWEEN '${dateBefore12Week}' AND '${todaysDate}' ${extraSearchQry}`
    dbRes = await call({ query }, 'makeQuery', 'get');
    response["totalOverDue12Week"] = dbRes.message?.[0]?.["totalOverDue12Week"] || 0

    res.send({
      success: true,
      message: response
    })
}
catch (error) {
  console.log("in getDisbursementStatsSummary error--->>", error)
  res.send({
    success: false,
    message: error
  })
}
}


exports.getTransactionHistoryForLCLimit = async (req, res, next) => {
  try {
    let reqBody = req.body
    let transactionHistory = []

    console.log(reqBody.docId,"here is docid-----")
   let query = `SELECT * FROM tbl_edocs WHERE docId = '${reqBody.docId}' `
    let limitDbResp = await call({ query }, 'makeQuery', 'get');

    // limit created on
    if(limitDbResp.message?.[0]){
      let temp = limitDbResp.message?.[0]
      transactionHistory.push({
        action: `Applied for ${limitDbResp.message?.[0].template}`,
        dateTime: temp.createdAt,
        date: temp.createdAt ? moment(temp.createdAt).format("DD MMM, YYYY") : "NA",
        time: temp.createdAt ? moment(temp.createdAt).format("hh:mm a") : "NA"
      })
    }


    // For atFinance Stage start
   
    // For atFinance Stage end

    // Sorting the audit log by datetime
    if(transactionHistory.length){
      transactionHistory.sort(function(a, b) {
        return a.dateTime - b.dateTime;
      });
      transactionHistory.reverse()
    }
    res.send({
      success: true,
      message: transactionHistory
    })
  }
  catch (error) {
    console.log("in getTransactionHistoryForInvoiceLimit error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}