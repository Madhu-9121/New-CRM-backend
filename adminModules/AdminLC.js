const { dbPool } = require("../../src/database/mysql")
const { call } = require("../../utils/codeHelper")
const moment = require("moment");
const fs = require('fs');
const e = require("cors");
const { nullCheck } = require("../../database/utils/utilFuncs");
const { count } = require("console");
const { sendMail } = require("../../utils/mailer");
const config = require("../../config");
const { emailEnabledBanks, enabledFinanciersForLC } = require("../../urlCostants");
const { getCurrentTimeStamp } = require("../../iris_server/utils");

const LCPurposeObjectV2 = {
  "lc_discounting": "Discounting (International)",
  "lc_confirmation": "Confirmation (International)",
  "lc_confirmation_discounting": "LC Confirmation & Discounting (International)",
  "lc_discounting_domestic": "Discounting (Domestic)",
  "lc_confirmation_domestic": "Confirmation (Domestic)",
  "lc_confirmation_discounting_domestic": "LC Confirmation & Discounting (Domestic)",
  "sblc": "SBLC"
}

exports.manageLCLogs = async (applicationId, status) => {
  return new Promise(async (resolve) => {
    // 1 - quoteSelectedOn
    // 2 - quoteLockedOn
    // 3 - contractDocsSentOn
    // 4 - contractDocsSignByExporterOn
    // 5 - contractDocsSignByFinancierOn

    // Check if alredy exist then update
    let query = `SELECT * FROM tbl_lc_audit_logs WHERE applicationId = '${applicationId}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    if (dbRes.message.length) {
      query = ` UPDATE tbl_lc_audit_logs SET `
      let subQuery = ""
      if (status / 1 == 1) {
        subQuery = ` quoteSelectedOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 2){
        subQuery = ` quoteLockedOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 3){
        subQuery = ` contractDocsSentOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 4){
        subQuery = ` contractDocsSignByExporterOn = '${getCurrentTimeStamp()}' `
      }
      if (status / 1 == 5){
        subQuery = ` contractDocsSignByFinancierOn = '${getCurrentTimeStamp()}' `
      }
      await dbPool.query(query + subQuery)
      return resolve(true)
    }
    // if not exist create and update
    else {
      query = ` INSERT INTO tbl_lc_audit_logs `
      let subQuery = ""
      if (status / 1 == 1) {
        subQuery = ` (applicationId, quoteSelectedOn) VALUES ('${applicationId}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 2){
        subQuery = ` (applicationId, quoteLockedOn) VALUES ('${applicationId}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 3){
        subQuery = ` (applicationId, contractDocsSentOn) VALUES ('${applicationId}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 4){
        subQuery = ` (applicationId, contractDocsSignByExporterOn) VALUES ('${applicationId}', '${getCurrentTimeStamp()}') `
      }
      if (status / 1 == 5){
        subQuery = ` (applicationId, contractDocsSignByFinancierOn) VALUES ('${applicationId}', '${getCurrentTimeStamp()}') `
      }
      await dbPool.query(query + subQuery)
      return resolve(true)
    }
  })
}

exports.getLCListForAdmin = async (req, res, next) => {
  try {
    let reqBody = req.body
    // Will be dynamic once financier selection option will be their
    if(reqBody.atLimitStage){

      let sortString = ` ORDER BY tbl_buyer_required_lc_limit.createdAt DESC `
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

      if (reqBody.dateRangeFilter) {
        if (reqBody.dateRangeFilter[0] && !reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_buyer_required_lc_limit.createdAt >= '${reqBody.dateRangeFilter[0]}' `
        }
        else if (!reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_buyer_required_lc_limit.createdAt <= '${reqBody.dateRangeFilter[1]}'  `
        }
        else if (reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
          extraSearchQry = ` AND tbl_buyer_required_lc_limit.createdAt BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' `
        }
      }

      if(reqBody.onlyShowForUserId){
        extraSearchQry += ` AND ( tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}')`
      }
      if(reqBody.subadminIds){
        extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }
      
      // if (reqBody.financiersFilter) {
      //   let lastIndex = reqBody.financiersFilter.length - 1
      //   for (let index = 0; index < reqBody.financiersFilter.length; index++) {
      //     const element = reqBody.financiersFilter[index];
      //     if(index == 0){
      //       havingSearchQry += ` ( ` 
      //     }
      //     havingSearchQry += ` selectedLenderName LIKE '%${element}%' ${lastIndex!=index ? ' OR ' : ''} `
      //     if (lastIndex == index){
      //       havingSearchQry += ` ) `
      //     }
      //   }
      // }
      if(havingSearchQry===" HAVING "){
        havingSearchQry = ""
      }
      if(reqBody.status){
        let isUnderReview = reqBody.status.includes("Under Review")
        let isRejected = reqBody.status.includes("Rejected")
        let isApproved = reqBody.status.includes("Approved")
        let isInprogress = reqBody.status.includes("Inprogress")
        if(isUnderReview){
          extraSearchQry +=  ` AND tbl_buyer_required_lc_limit.reviewPending = 1 `
        }
        if(isRejected){
          havingSearchQry = !havingSearchQry ? " HAVING " : (havingSearchQry )
          if(havingSearchQry!=" HAVING "){
            havingSearchQry += ` ${reqBody.status.length/1 == 1  ? ' AND ' : ' OR ' } `
          }
          havingSearchQry += ` (countOfDeniedQuotes = countOfSelectedLender) `
        }
        if(isApproved){
          extraSearchQry += ` ${reqBody.status.length/1 == 1 ? ' AND ' : ' OR '} (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 1 AND 
            tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 1) `
        }
        if(isInprogress){
          havingSearchQry = !havingSearchQry ? " HAVING " : (havingSearchQry )
          if(havingSearchQry!=" HAVING "){
            havingSearchQry += ` ${reqBody.status.length/1 == 1  ? ' AND ' : ' OR ' } `
          }
          havingSearchQry += ` (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) `
          extraSearchQry += ` AND (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 0 OR 
            tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 0) `
        }
      }
      if(havingSearchQry===" HAVING "){
        havingSearchQry = ""
      }
      if(reqBody.search){
        searchQuery = ` AND (tbl_buyers_detail.buyerName LIKE '%${reqBody.search}%' OR supplierDetails.company_name LIKE '%${reqBody.search}%' 
        OR tbl_buyer_required_lc_limit.lcNo LIKE '%${reqBody.search}%' ) `
      }

      if(reqBody.resultPerPage && reqBody.currentPage) {
        perPageString = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage}`;
      } 
      
      if(reqBody.sortDateBy){
        sortString = ` ORDER BY tbl_buyer_required_lc_limit.createdAt ${reqBody.sortDateBy} `
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
      tbl_countries.name AS countryName,
      (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"denied"', '')))/LENGTH('"status":"denied"') AS countOfDeniedQuotes,
      (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"approved"', '')))/LENGTH('"status":"approved"') AS countOfApprovedQuotes,
      tbl_buyer_required_lc_limit.id AS applicationId, tbl_buyer_required_lc_limit.expectedDateOfShipment,
      tbl_buyer_required_lc_limit.lcNo, tbl_buyer_required_lc_limit.invRefNo,
      tbl_buyer_required_lc_limit.lcPurpose, tbl_buyer_required_lc_limit.financierQuotes,
      tbl_buyer_required_lc_limit.selectedFinancier, tbl_buyer_required_lc_limit.quoteLocked,
      tbl_buyer_required_lc_limit.sameBankForLCDiscounting,
      tbl_buyer_required_lc_limit.updatedAt AS applicationUpdatedAt,
      tbl_buyer_required_lc_limit.createdAt AS applicationCreatedAt,   
      tbl_buyer_required_lc_limit.contractDocsFromFinanciers, 
      tbl_buyer_required_lc_limit.contractDocsSignedByExporter,
      tbl_buyer_required_lc_limit.contractDocsSignedByFinancier,
      tbl_buyer_required_lc_limit.reviewPending,
      tbl_buyer_required_lc_limit.lcTenor,
      tbl_buyer_required_lc_limit.ocrFields,
      tbl_buyer_required_lc_limit.lcIssuingBankName, 
      tbl_buyer_required_lc_limit.shipmentToCountry,
      supplierDetails.tbl_user_id AS supplierUserId,
      supplierDetails.company_name AS supplierName,
      supplierDetails.email_id AS supplierEmailId,
      supplierCountry.name AS supplierCountryName,
      GROUP_CONCAT(DISTINCT lenderDetails.company_name ORDER BY lenderDetails.tbl_user_id) AS selectedLenderName,
      GROUP_CONCAT(DISTINCT lenderDetails.tbl_user_id ORDER BY lenderDetails.tbl_user_id) AS selectedLenderId,
      COUNT(lenderDetails.company_name REGEXP ',') as countOfSelectedLender,

      tbl_buyer_required_lc_limit.buyerId,
      tbl_buyer_required_lc_limit.lcPurpose,
      tbl_buyer_required_lc_limit.lcType,

      adminDetails.contact_person AS leadAssignToName,
      adminDetails.tbl_user_id AS leadAssignToId,

      destCountry.name AS shipmentToCountryName,
      
      (
        SELECT GROUP_CONCAT(chat_room_id SEPARATOR ',')
        FROM tbl_chat_rooms
        WHERE tbl_chat_rooms.lcApplicationId = tbl_buyer_required_lc_limit.id
      ) AS chatRoomIds,
      (
        SELECT GROUP_CONCAT(included_users SEPARATOR ',')
        FROM tbl_chat_rooms
        WHERE tbl_chat_rooms.lcApplicationId = tbl_buyer_required_lc_limit.id
      ) AS chatRoomUsers,
      (
        SELECT GROUP_CONCAT(
            COALESCE(tbl_chatroom_unread_msg.count, '0')
            ORDER BY tbl_chat_rooms.chat_room_id
            SEPARATOR ','
        )
        FROM tbl_chat_rooms
        LEFT JOIN tbl_chatroom_unread_msg ON tbl_chat_rooms.chat_room_id = tbl_chatroom_unread_msg.chatRoomId
            AND tbl_chatroom_unread_msg.userId = '${reqBody.userId}'
        WHERE tbl_chat_rooms.lcApplicationId = tbl_buyer_required_lc_limit.id
      ) AS chatRoomUnreadMsgCount,

      (SELECT tbl_admin_remarks.remark FROM tbl_admin_remarks
      WHERE tbl_admin_remarks.lcApplicationId = tbl_buyer_required_lc_limit.id
      ORDER BY tbl_admin_remarks.id DESC LIMIT 1
      ) AS lastInternalRemark,
      GROUP_CONCAT(DISTINCT IFNULL(tbl_last_message.id, 'null') ORDER BY chat_rooms.chat_room_id) AS lastMessageIds  

      FROM tbl_buyer_required_lc_limit
      LEFT JOIN tbl_buyers_detail ON
      tbl_buyer_required_lc_limit.buyerId = tbl_buyers_detail.id
      LEFT JOIN tbl_chat_rooms AS chat_rooms ON chat_rooms.lcApplicationId = tbl_buyer_required_lc_limit.id
      LEFT JOIN tbl_last_message ON tbl_last_message.chat_room_id = chat_rooms.chat_room_id  
      LEFT JOIN tbl_countries 
      ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_buyer_required_lc_limit.createdBy = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_countries destCountry ON
      destCountry.sortname COLLATE utf8mb4_unicode_ci = tbl_buyer_required_lc_limit.shipmentToCountry COLLATE utf8mb4_unicode_ci
      LEFT JOIN tbl_countries supplierCountry ON
      supplierDetails.country_code = supplierCountry.sortname
      LEFT JOIN tbl_share_lc_quote_request ON
      tbl_buyer_required_lc_limit.id = tbl_share_lc_quote_request.quoteId
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_share_lc_quote_request.lenderId = lenderDetails.tbl_user_id
      WHERE 1
      ${searchQuery} ${extraSearchQry}
      GROUP BY tbl_buyer_required_lc_limit.id
      ${havingSearchQry}
      ${sortString} ${perPageString}`;


      let countQuery = `SELECT tbl_buyers_detail.id,
      (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"denied"', '')))/LENGTH('"status":"denied"') AS countOfDeniedQuotes,
      (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"approved"', '')))/LENGTH('"status":"approved"') AS countOfApprovedQuotes,
      GROUP_CONCAT(DISTINCT lenderDetails.company_name ORDER BY lenderDetails.tbl_user_id) AS selectedLenderName,
      COUNT(lenderDetails.company_name REGEXP ',') as countOfSelectedLender      

      FROM tbl_buyer_required_lc_limit
      LEFT JOIN tbl_buyers_detail ON
      tbl_buyer_required_lc_limit.buyerId = tbl_buyers_detail.id
      LEFT JOIN tbl_countries 
      ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_buyer_required_lc_limit.createdBy = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_share_lc_quote_request ON
      tbl_buyer_required_lc_limit.id = tbl_share_lc_quote_request.quoteId
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_share_lc_quote_request.lenderId = lenderDetails.tbl_user_id
      WHERE 1
      ${searchQuery} ${extraSearchQry} 
      GROUP BY tbl_buyer_required_lc_limit.id
      ${havingSearchQry}
      ${sortString}`;

      let dbRes = await call({ query }, 'makeQuery', 'get');
      let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');

      let filterCount = {}

      // Inprogress
      havingSearchQry = ` (countOfDeniedQuotes IS NULL OR countOfDeniedQuotes != countOfSelectedLender) `
      extraSearchQry = ` AND (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 0 OR 
        tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 0) `
        if(reqBody.onlyShowForUserId){
          extraSearchQry += ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
        }
        if(reqBody.subadminIds){
          extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
        }
      let filterQuery = `SELECT tbl_buyer_required_lc_limit.id,
          (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"denied"', '')))/LENGTH('"status":"denied"') AS countOfDeniedQuotes,
          GROUP_CONCAT(lenderDetails.company_name) AS selectedLenderName,
          COUNT(lenderDetails.company_name REGEXP ',') as countOfSelectedLender 
          
          FROM tbl_buyer_required_lc_limit

          LEFT JOIN tbl_share_lc_quote_request ON
          tbl_buyer_required_lc_limit.id = tbl_share_lc_quote_request.quoteId
          LEFT JOIN tbl_user_details lenderDetails ON
          tbl_share_lc_quote_request.lenderId = lenderDetails.tbl_user_id
          LEFT JOIN tbl_user ON
          tbl_buyer_required_lc_limit.createdBy = tbl_user.id         
          LEFT JOIN tbl_user_details adminDetails ON
          adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
          WHERE 1
          ${extraSearchQry} 
          GROUP BY tbl_buyer_required_lc_limit.id
          HAVING ${havingSearchQry}`;

      let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      filterCount["inprogress"] = filterDbRes.message.length

      
      // Under Review
      filterQuery = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit 
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_buyer_required_lc_limit.createdBy = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      WHERE tbl_buyer_required_lc_limit.reviewPending = 1 ${extraSearchQry}`
      filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      filterCount["underreview"] = filterDbRes.message.length

      // Approved
      extraSearchQry = ` (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 1 AND 
        tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 1) `
        if(reqBody.onlyShowForUserId){
          extraSearchQry += ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
        }
        if(reqBody.subadminIds){
          extraSearchQry += ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
        }
      filterQuery = `SELECT tbl_buyer_required_lc_limit.id
      FROM tbl_buyer_required_lc_limit
      LEFT JOIN tbl_user_details supplierDetails ON
      tbl_buyer_required_lc_limit.createdBy = supplierDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      supplierDetails.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      WHERE 1 AND ${extraSearchQry}`;
      filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      filterCount["approved"] = filterDbRes.message.length

      // Rejected
      havingSearchQry = ` (countOfDeniedQuotes = countOfSelectedLender) `
      extraSearchQry = ''
      if(reqBody.onlyShowForUserId){
        extraSearchQry = ` AND adminDetails.tbl_user_id = '${reqBody.onlyShowForUserId}'`
      }
      if(reqBody.subadminIds){
        extraSearchQry = ` AND adminDetails.tbl_user_id IN ('${reqBody.subadminIds?.join("','")}')`
      }
      filterQuery = `SELECT tbl_buyer_required_lc_limit.id,
      (LENGTH(tbl_buyer_required_lc_limit.financierQuotes) - LENGTH(REPLACE(tbl_buyer_required_lc_limit.financierQuotes, '"status":"denied"', '')))/LENGTH('"status":"denied"') AS countOfDeniedQuotes,
      GROUP_CONCAT(lenderDetails.company_name) AS selectedLenderName,
      COUNT(lenderDetails.company_name REGEXP ',') as countOfSelectedLender 
      
      FROM tbl_buyer_required_lc_limit

      LEFT JOIN tbl_share_lc_quote_request ON
      tbl_buyer_required_lc_limit.id = tbl_share_lc_quote_request.quoteId
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_share_lc_quote_request.lenderId = lenderDetails.tbl_user_id
      LEFT JOIN tbl_user ON
      tbl_buyer_required_lc_limit.createdBy = tbl_user.id      
      LEFT JOIN tbl_user_details adminDetails ON
      adminDetails.tbl_user_id = tbl_user.LeadAssignedTo
      WHERE 1 ${extraSearchQry}
      GROUP BY tbl_buyer_required_lc_limit.id
      HAVING ${havingSearchQry}`;

      filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      filterCount["rejected"] = filterDbRes.message.length

      res.send({success: true, message: {filterCount, listData: dbRes.message, countData: countDbRes.message.length}});
    }

    // if(reqBody.atFinanceStage){

    //   let sortString = ` ORDER BY tbl_invoice_discounting.created_at DESC `
    //   let havingSearchQry = " HAVING " 
    //   let searchQuery = ""
    //   let perPageString = "";
    //   let extraSearchQry = "";

    //   if(reqBody.sortDateBy){
    //     sortString = ` ORDER BY tbl_invoice_discounting.created_at ${reqBody.sortDateBy} `
    //   }
      
    //   if(reqBody.sortExpName){
    //     sortString = ` ORDER BY supplierDetails.company_name ${reqBody.sortExpName} `
    //   }

    //   if(reqBody.sortBuyerName){
    //     sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortBuyerName} `
    //   }

    //   if(reqBody.search){
    //     searchQuery = ` AND (tbl_buyers_detail.buyerName LIKE '%${reqBody.search}%' OR supplierDetails.company_name LIKE '%${reqBody.search}%' 
    //     OR tbl_invoice_discounting.reference_no LIKE '%${reqBody.search}%' OR tbl_invoice_discounting.stenn_deal_id LIKE '%${reqBody.search}%' OR 
    //     tbl_invoice_discounting.modifi_deal_id LIKE '%${reqBody.search}%' ) `
    //   }

    //   if(reqBody.resultPerPage && reqBody.currentPage) {
    //     perPageString = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage}`;
    //   } 

    //   if(havingSearchQry === " HAVING "){
    //     havingSearchQry = ""
    //   }

    //   if(reqBody.buyerName){
    //     extraSearchQry += ` AND tbl_buyers_detail.buyerName IN (${reqBody.buyerName.join(",")}) `
    //   }

    //   if(reqBody.exporterName){
    //     extraSearchQry += ` AND supplierDetails.company_name IN (${reqBody.exporterName.join(",")}) `
    //   }

    //   if(reqBody.financiersFilter){
    //     extraSearchQry += ` AND lenderDetails.company_name IN (${reqBody.financiersFilter.join(",")}) `
    //   }

    //   if (reqBody.dateRangeFilter) {
    //     if (reqBody.dateRangeFilter[0] && !reqBody.dateRangeFilter[1]) {
    //       extraSearchQry = ` AND tbl_invoice_discounting.created_at >= '${reqBody.dateRangeFilter[0]}' `
    //     }
    //     else if (!reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
    //       extraSearchQry = ` AND tbl_invoice_discounting.created_at <= '${reqBody.dateRangeFilter[1]}'  `
    //     }
    //     else if (reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
    //       extraSearchQry = ` AND tbl_invoice_discounting.created_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' `
    //     }
    //   }

    //   if (reqBody.status) {
    //     let isApplied = reqBody.status.includes("Applied")
    //     let isInprogress = reqBody.status.includes("Inprogress")
    //     let isApproved = reqBody.status.includes("Approved")
    //     let isRejected = reqBody.status.includes("Rejected")
    //     let isDisbursed = reqBody.status.includes("Disbursed")
    //     let lastElement = reqBody.status[reqBody.status.length - 1]
    //     // 3 - approved
    //     // 5 - rejected
    //     // 4 - disbursed
    //     // 6 - In progress
    //     extraSearchQry += ` AND `
    //     if(isApplied){
    //       extraSearchQry += ` (tbl_invoice_discounting.status !=3 AND tbl_invoice_discounting.status !=4 AND 
    //       tbl_invoice_discounting.status !=5 AND tbl_invoice_discounting.status !=6) 
    //       ${lastElement === "Applied" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
    //     }
    //     // if(isInprogress){
    //     //   extraSearchQry += `  tbl_invoice_discounting.status = 6 
    //     //   ${lastElement === "Inprogress" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
    //     // }
    //     if(isApproved){
    //       extraSearchQry += ` (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6 )
    //       ${lastElement === "Approved" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
    //     }
    //     if(isRejected){
    //       extraSearchQry += ` tbl_invoice_discounting.status = 5 
    //       ${lastElement === "Rejected" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
    //     }
    //     // if(isDisbursed){
    //     //   extraSearchQry += ` tbl_invoice_discounting.status = 4
    //     //   ${lastElement === "Disbursed" ? " " : (reqBody.status.length/1 > 1 ? ' OR ' : ' AND ')} `
    //     // }
    //   }

    //   let query = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id,
    //   tbl_buyers_detail.*,
    //   tbl_buyer_required_limit.id AS applicationId, tbl_buyer_required_limit.expShipmentDate,
    //   tbl_buyer_required_limit.invRefNo,
    //   tbl_invoice_discounting.reference_no,
    //   tbl_invoice_discounting.stenn_deal_id,
    //   tbl_invoice_discounting.created_at AS invoiceApplicationCreatedAt,
    //   tbl_invoice_discounting.status AS invoiceStatus,
    //   tbl_buyer_required_limit.updatedAt AS applicationUpdatedAt,
    //   tbl_buyer_required_limit.createdAt AS applicationCreatedAt,   
    //   tbl_buyer_required_limit.termSheet, 
    //   tbl_buyer_required_limit.termSheetSignedByExporter,
    //   tbl_buyer_required_limit.termSheetSignedByBank,  
    //   tbl_buyer_required_limit.documentStatus,
    //   tbl_buyer_required_limit.selectedQuote,
    //   tbl_buyer_required_limit.frameworkDoc,
    //   tbl_buyer_required_limit.exhibitDoc,
    //   tbl_buyer_required_limit.noaDoc,
    //   tbl_buyer_required_limit.selectedQuote,

    //   frameworkSignStatus.signatureId AS frameworkExporterSign,
    //   frameworkSignStatus.financierSignatureId AS frameworkFinancierSign,
    //   frameworkSignStatus.buyerSignatureId AS frameworkBuyerSign,

    //   exhibitSignStatus.signatureId AS exhibitExporterSign,
    //   exhibitSignStatus.financierSignatureId AS exhibitFinancierSign,
    //   exhibitSignStatus.buyerSignatureId AS exhibitBuyerSign,

    //   noaSignStatus.signatureId AS noaExporterSign,
    //   noaSignStatus.financierSignatureId noaFinancierSign,
    //   noaSignStatus.buyerSignatureId noaBuyerSign,

    //   supplierDetails.company_name AS supplierName,
    //   lenderDetails.company_name AS lenderName
    //   FROM tbl_invoice_discounting 
    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   LEFT JOIN tbl_buyers_detail ON
    //   tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   LEFT JOIN tbl_user_details supplierDetails ON
    //   tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   LEFT JOIN tbl_user_details lenderDetails ON
    //   tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id

    //   LEFT JOIN tbl_document_details frameworkSignStatus ON
    //   tbl_buyer_required_limit.frameworkDoc = frameworkSignStatus.id

    //   LEFT JOIN tbl_document_details exhibitSignStatus ON
    //   tbl_buyer_required_limit.exhibitDoc = exhibitSignStatus.id

    //   LEFT JOIN tbl_document_details noaSignStatus ON
    //   tbl_buyer_required_limit.noaDoc = noaSignStatus.id

    //   WHERE 1 
    //   ${searchQuery} ${extraSearchQry} ${havingSearchQry}
    //   ${sortString} ${perPageString}` ;

    //   console.log('query atFinanceStage =========================>', query);

    //   let countQuery = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    //   FROM tbl_invoice_discounting 
    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   LEFT JOIN tbl_buyers_detail ON
    //   tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   LEFT JOIN tbl_user_details supplierDetails ON
    //   tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   LEFT JOIN tbl_user_details lenderDetails ON
    //   tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    //   WHERE 1 
    //   ${searchQuery} ${extraSearchQry} ${havingSearchQry}
    //   ${sortString}` ;

    //   let dbRes = await call({ query }, 'makeQuery', 'get');
    //   let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');

    //   let filterCount = {}
    //   // Applied
    //   let filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    //   FROM tbl_invoice_discounting 
    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   LEFT JOIN tbl_buyers_detail ON
    //   tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   LEFT JOIN tbl_user_details supplierDetails ON
    //   tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   LEFT JOIN tbl_user_details lenderDetails ON
    //   tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    //   WHERE tbl_invoice_discounting.status != 3 AND tbl_invoice_discounting.status != 4 AND tbl_invoice_discounting.status != 5 AND 
    //   tbl_invoice_discounting.status != 6 `
    //   filterCount["applied"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
    //   filterCount["applied"] = filterCount["applied"].message.length; 
    //   // Approved
    //   filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    //   FROM tbl_invoice_discounting 
    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   LEFT JOIN tbl_buyers_detail ON
    //   tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   LEFT JOIN tbl_user_details supplierDetails ON
    //   tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   LEFT JOIN tbl_user_details lenderDetails ON
    //   tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    //   WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) `
    //   filterCount["approved"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
    //   filterCount["approved"] = filterCount["approved"].message.length; 
    //   // Rejected
    //   filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    //   FROM tbl_invoice_discounting 
    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   LEFT JOIN tbl_buyers_detail ON
    //   tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   LEFT JOIN tbl_user_details supplierDetails ON
    //   tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   LEFT JOIN tbl_user_details lenderDetails ON
    //   tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    //   WHERE tbl_invoice_discounting.status = 5 `
    //   filterCount["rejected"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
    //   filterCount["rejected"] = filterCount["rejected"].message.length; 
    //   // Inprogress
    //   // filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    //   // FROM tbl_invoice_discounting 
    //   // LEFT JOIN tbl_buyer_required_limit ON
    //   // tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   // LEFT JOIN tbl_buyers_detail ON
    //   // tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   // LEFT JOIN tbl_user_details supplierDetails ON
    //   // tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   // LEFT JOIN tbl_user_details lenderDetails ON
    //   // tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    //   // WHERE tbl_invoice_discounting.status = 6 `
    //   // filterCount["inprogress"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
    //   // filterCount["inprogress"] = filterCount["inprogress"].message.length; 
    //   // Disbursed
    //   // filterCountQry = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    //   // FROM tbl_invoice_discounting 
    //   // LEFT JOIN tbl_buyer_required_limit ON
    //   // tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   // LEFT JOIN tbl_buyers_detail ON
    //   // tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   // LEFT JOIN tbl_user_details supplierDetails ON
    //   // tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   // LEFT JOIN tbl_user_details lenderDetails ON
    //   // tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    //   // WHERE tbl_invoice_discounting.status = 4 `
    //   // filterCount["disbursed"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
    //   // filterCount["disbursed"] = filterCount["disbursed"].message.length; 

    //   res.send({success: true, message: { filterCount, listData: dbRes.message, countData: countDbRes.message.length}});
    // }

    // if(reqBody.atApprovedStage){
    //   let sortString = ` ORDER BY tbl_invoice_discounting.created_at DESC `
    //   let havingSearchQry = " HAVING " 
    //   let searchQuery = ""
    //   let perPageString = "";
    //   let extraSearchQry = "";

    //   if(reqBody.search){
    //     searchQuery = ` AND (tbl_buyers_detail.buyerName LIKE '%${reqBody.search}%' OR supplierDetails.company_name LIKE '%${reqBody.search}%' 
    //     OR tbl_invoice_discounting.reference_no LIKE '%${reqBody.search}%' OR tbl_invoice_discounting.stenn_deal_id LIKE '%${reqBody.search}%' OR 
    //     tbl_invoice_discounting.modifi_deal_id LIKE '%${reqBody.search}%' ) `
    //   }

    //   if(reqBody.sortExpName){
    //     sortString = ` ORDER BY supplierDetails.company_name ${reqBody.sortExpName} `
    //   }

    //   if(reqBody.sortBuyerName){
    //     sortString = ` ORDER BY tbl_buyers_detail.buyerName ${reqBody.sortBuyerName} `
    //   }

    //   if(havingSearchQry === " HAVING "){
    //     havingSearchQry = ""
    //   }

    //   if(reqBody.buyerName){
    //     extraSearchQry += ` AND tbl_buyers_detail.buyerName IN (${reqBody.buyerName.join(",")}) `
    //   }

    //   if(reqBody.exporterName){
    //     extraSearchQry += ` AND supplierDetails.company_name IN (${reqBody.exporterName.join(",")}) `
    //   }

    //   if(reqBody.financiersFilter){
    //     extraSearchQry += ` AND lenderDetails.company_name IN (${reqBody.financiersFilter.join(",")}) `
    //   }

    //   if (reqBody.dateRangeFilter) {
    //     if (reqBody.dateRangeFilter[0] && !reqBody.dateRangeFilter[1]) {
    //       extraSearchQry = ` AND tbl_invoice_discounting.created_at >= '${reqBody.dateRangeFilter[0]}' `
    //     }
    //     else if (!reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
    //       extraSearchQry = ` AND tbl_invoice_discounting.created_at <= '${reqBody.dateRangeFilter[1]}'  `
    //     }
    //     else if (reqBody.dateRangeFilter[0] && reqBody.dateRangeFilter[1]) {
    //       extraSearchQry = ` AND tbl_invoice_discounting.created_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' `
    //     }
    //   }

    //   let query = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id,
    //   tbl_buyers_detail.*,
    //   tbl_buyer_required_limit.id AS applicationId, tbl_buyer_required_limit.expShipmentDate,
    //   tbl_buyer_required_limit.invRefNo,
    //   tbl_invoice_discounting.reference_no,
    //   tbl_invoice_discounting.stenn_deal_id,
    //   tbl_invoice_discounting.created_at AS invoiceApplicationCreatedAt,
    //   tbl_invoice_discounting.status AS invoiceStatus,
    //   tbl_buyer_required_limit.updatedAt AS applicationUpdatedAt,
    //   tbl_buyer_required_limit.createdAt AS applicationCreatedAt,   
    //   tbl_buyer_required_limit.termSheet, 
    //   tbl_buyer_required_limit.termSheetSignedByExporter,
    //   tbl_buyer_required_limit.termSheetSignedByBank,  
    //   tbl_buyer_required_limit.documentStatus,
    //   tbl_buyer_required_limit.selectedQuote,
    //   tbl_buyer_required_limit.frameworkDoc,
    //   tbl_buyer_required_limit.exhibitDoc,
    //   tbl_buyer_required_limit.noaDoc,
    //   tbl_buyer_required_limit.selectedQuote,

    //   frameworkSignStatus.signatureId AS frameworkExporterSign,
    //   frameworkSignStatus.financierSignatureId AS frameworkFinancierSign,
    //   frameworkSignStatus.buyerSignatureId AS frameworkBuyerSign,

    //   exhibitSignStatus.signatureId AS exhibitExporterSign,
    //   exhibitSignStatus.financierSignatureId AS exhibitFinancierSign,
    //   exhibitSignStatus.buyerSignatureId AS exhibitBuyerSign,

    //   noaSignStatus.signatureId AS noaExporterSign,
    //   noaSignStatus.financierSignatureId noaFinancierSign,
    //   noaSignStatus.buyerSignatureId noaBuyerSign,

    //   supplierDetails.company_name AS supplierName,
    //   lenderDetails.company_name AS lenderName,
    //   GROUP_CONCAT(tbl_disbursement_scheduled.scheduledOn) AS disbScheduledOn,
    //   GROUP_CONCAT(tbl_disbursement_scheduled.amount) AS disbAmount,
    //   GROUP_CONCAT(tbl_disbursement_scheduled.currency) AS disbCurrency,
    //   GROUP_CONCAT(tbl_disbursement_scheduled.status) AS disbStatus,
    //   GROUP_CONCAT(IFNULL(tbl_disbursement_scheduled.disbursedAmount, 'NA')) AS disbActualAmount,
    //   GROUP_CONCAT(IFNULL(tbl_disbursement_scheduled.updatedAt, 'NA')) AS disbActualDate,
    //   GROUP_CONCAT(IFNULL(tbl_disbursement_scheduled.attachment, 'NA')) AS disbAttachment
    //   FROM tbl_invoice_discounting 

    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   LEFT JOIN tbl_buyers_detail ON
    //   tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   LEFT JOIN tbl_user_details supplierDetails ON
    //   tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   LEFT JOIN tbl_user_details lenderDetails ON
    //   tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id

    //   LEFT JOIN tbl_document_details frameworkSignStatus ON
    //   tbl_buyer_required_limit.frameworkDoc = frameworkSignStatus.id

    //   LEFT JOIN tbl_document_details exhibitSignStatus ON
    //   tbl_buyer_required_limit.exhibitDoc = exhibitSignStatus.id

    //   LEFT JOIN tbl_document_details noaSignStatus ON
    //   tbl_buyer_required_limit.noaDoc = noaSignStatus.id

    //   LEFT JOIN tbl_disbursement_scheduled ON
    //   tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo

    //   WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6)
    //   ${searchQuery} ${extraSearchQry}
    //   GROUP BY tbl_invoice_discounting.reference_no
    //   ${havingSearchQry}
    //   ${sortString} ${perPageString}` ;

    //   console.log('query atApprovedStage =========================>', query);

    //   let countQuery = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id
    //   FROM tbl_invoice_discounting 
    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo
    //   LEFT JOIN tbl_buyers_detail ON
    //   tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
    //   LEFT JOIN tbl_user_details supplierDetails ON
    //   tbl_invoice_discounting.seller_id = supplierDetails.tbl_user_id
    //   LEFT JOIN tbl_user_details lenderDetails ON
    //   tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
    //   LEFT JOIN tbl_disbursement_scheduled ON
    //   tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
    //   WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) 
    //   ${searchQuery} ${extraSearchQry}
    //   GROUP BY tbl_invoice_discounting.reference_no ${havingSearchQry}
    //   ${sortString}` ;

    //   let dbRes = await call({ query }, 'makeQuery', 'get');
    //   let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');

    //   let filterCount = {}

    //   res.send({success: true, message: { filterCount, listData: dbRes.message, countData: countDbRes.message.length}});
    // }
  }
  catch (error) {
    console.log("in getLCListForAdmin error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getLCFiltersForAdmin = async (req, res, next) => {
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
      LEFT JOIN tbl_buyer_required_lc_limit ON
      tbl_user_details.tbl_user_id = tbl_buyer_required_lc_limit.createdBy
      WHERE tbl_buyer_required_lc_limit.buyerId IS NOT NULL
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
      LEFT JOIN tbl_buyer_required_lc_limit ON
      tbl_buyers_detail.id = tbl_buyer_required_lc_limit.buyerId
      WHERE tbl_buyer_required_lc_limit.buyerId IS NOT NULL
      ORDER BY tbl_buyers_detail.buyerName ASC `
    }
    if(reqBody.atFinanceStage){
      query = `SELECT DISTINCT tbl_buyers_detail.buyerName AS name FROM tbl_invoice_discounting
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      ORDER BY tbl_buyers_detail.buyerName ASC `
    }
    if(reqBody.atApprovedStage){
      query = `SELECT DISTINCT tbl_buyers_detail.buyerName AS name FROM tbl_invoice_discounting
      LEFT JOIN tbl_buyers_detail ON
      tbl_invoice_discounting.buyer_id = tbl_buyers_detail.id
      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6)
      ORDER BY tbl_buyers_detail.buyerName ASC `
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
      data: await enabledFinanciersForLC(),
      labelName: "name"
    }
    //
    filterData["Status"] = {
      "accordianId": 'status',
      type: "checkbox",
      labelName: "name"
    }
    if(reqBody.atLimitStage){
      filterData["Status"]["data"] = [{name: "Under Review"}, {name: "Inprogress"},{name: "Approved"}, {name: 'Rejected'}]
    }
    if(reqBody.atFinanceStage){
      // filterData["Status"]["data"] = [{name: "Applied"},{name: "Inprogress"}, {name: 'Approved'}, {name: 'Rejected'},
      // {name: 'Disbursed'}]
      filterData["Status"]["data"] = [ {name: "Applied"},{name: 'Approved'}, {name: 'Rejected'}]
    }
    if(reqBody.atApprovedStage){
      delete filterData["Status"]
    }
    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("in getLCFiltersForAdmin error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getTransactionHistoryForLC = async (req, res, next) => {
  try {
    let reqBody = req.body
    let transactionHistory = []

    let query = `SELECT * FROM tbl_buyer_required_lc_limit WHERE id = '${reqBody.applicationId}' `
    let limitDbResp = await call({ query }, 'makeQuery', 'get');
    let selectedQuote = JSON.parse(limitDbResp.message?.[0]?.selectedQuote || "{}")

    query = `SELECT * FROM tbl_lc_audit_logs WHERE applicationId = '${reqBody.applicationId}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    dbRes = dbRes.message[0] || {}

    // limit created on
    if(limitDbResp.message?.[0]){
      let temp = limitDbResp.message?.[0]
      transactionHistory.push({
        action: `Applied for LC ${LCPurposeObjectV2[temp.lcPurpose]}${temp.sameBankForLCDiscounting ? " & Discounting" : ""}`,
        dateTime: temp.createdAt,
        date: temp.createdAt ? moment(temp.createdAt).format("DD MMM, YYYY") : "NA",
        time: temp.createdAt ? moment(temp.createdAt).format("hh:mm a") : "NA"
      })
    }

    if(limitDbResp.message?.[0]?.financierQuotes){
      // quote details
      let temp = JSON.parse(limitDbResp.message?.[0]?.financierQuotes) || []
      for (let index = 0; index < temp.length; index++) {
        const element = temp[index];
        transactionHistory.push({
          action: element.status === "denied" ? `Quote denied by ${element.lender_name}` : `Quote approved by ${element.lender_name}`,
          dateTime: element.assignDate,
          date: element.assignDate ? moment(element.assignDate).format("DD MMM, YYYY") : "NA",
          time: element.assignDate ? moment(element.assignDate).format("hh:mm a") : "NA"
        })
      }
      // quote selected on
      if(limitDbResp.message?.[0]?.selectedQuote){
        transactionHistory.push({
          action: `Quote from ${selectedQuote.lender_name} selected by exporter`,
          dateTime: dbRes.quoteSelectedOn,
          date: dbRes.quoteSelectedOn ? moment(dbRes.quoteSelectedOn).format("DD MMM, YYYY") : "NA",
          time: dbRes.quoteSelectedOn ? moment(dbRes.quoteSelectedOn).format("hh:mm a") : "NA"
        })
      }
      // quote locked on
      if(limitDbResp.message?.[0]?.quoteLocked){
        transactionHistory.push({
          action: `Quote locked by ${selectedQuote.lender_name}`,
          dateTime: dbRes.quoteLockedOn,
          date: dbRes.quoteLockedOn ? moment(dbRes.quoteLockedOn).format("DD MMM, YYYY") : "NA",
          time: dbRes.quoteLockedOn ? moment(dbRes.quoteLockedOn).format("hh:mm a") : "NA"
        })
      }
      // contract docs sent on
      if(limitDbResp.message?.[0]?.contractDocsFromFinanciers!=null){
        transactionHistory.push({
          action: `Contract docs sent by ${selectedQuote.lender_name} `,
          dateTime: dbRes.contractDocsSentOn,
          date: dbRes.contractDocsSentOn ? moment(dbRes.contractDocsSentOn).format("DD MMM, YYYY") : "NA",
          time: dbRes.contractDocsSentOn ? moment(dbRes.contractDocsSentOn).format("hh:mm a") : "NA"
        })
      }
      // contract docs signed by financier
      if(limitDbResp.message?.[0]?.contractDocsSignedByFinancier){
        transactionHistory.push({
          action: `Contract docs signed by ${selectedQuote.lender_name} financier`,
          dateTime: dbRes.contractDocsSignByFinancierOn,
          date: dbRes.contractDocsSignByFinancierOn ? moment(dbRes.contractDocsSignByFinancierOn).format("DD MMM, YYYY") : "NA",
          time: dbRes.contractDocsSignByFinancierOn ? moment(dbRes.contractDocsSignByFinancierOn).format("hh:mm a") : "NA"
        })
      }
      // contract docs signed by supplier
      if(limitDbResp.message?.[0]?.contractDocsSignedByExporter){
        transactionHistory.push({
          action: `Contract docs signed by exporter `,
          dateTime: dbRes.contractDocsSignByExporterOn,
          date: dbRes.contractDocsSignByExporterOn ? moment(dbRes.contractDocsSignByExporterOn).format("DD MMM, YYYY") : "NA",
          time: dbRes.contractDocsSignByExporterOn ? moment(dbRes.contractDocsSignByExporterOn).format("hh:mm a") : "NA"
        })
      }
    } 

    // For atFinance Stage start
    // if(reqBody.invRefNo){
    //   // 3 - approved
    //   // 5 - rejected
    //   // 4 - disbursed
    //   // 6 - In progress
    //   query = `SELECT tbl_invoice_discounting.status, 
    //   tbl_invoice_discounting.created_at,
    //   tbl_invoice_discounting_audit_logs.*,

    //   tbl_buyer_required_limit.frameworkDoc,
    //   tbl_buyer_required_limit.exhibitDoc,
    //   tbl_buyer_required_limit.noaDoc,

    //   frameworkSignStatus.signatureId AS frameworkExporterSign,
    //   frameworkSignStatus.financierSignatureId AS frameworkFinancierSign,
    //   frameworkSignStatus.buyerSignatureId AS frameworkBuyerSign,

    //   exhibitSignStatus.signatureId AS exhibitExporterSign,
    //   exhibitSignStatus.financierSignatureId AS exhibitFinancierSign,
    //   exhibitSignStatus.buyerSignatureId AS exhibitBuyerSign,

    //   noaSignStatus.signatureId AS noaExporterSign,
    //   noaSignStatus.financierSignatureId noaFinancierSign,
    //   noaSignStatus.buyerSignatureId noaBuyerSign

    //   FROM tbl_invoice_discounting 

    //   LEFT JOIN tbl_invoice_discounting_audit_logs ON
    //   tbl_invoice_discounting.reference_no = tbl_invoice_discounting_audit_logs.invRefNo

    //   LEFT JOIN tbl_buyer_required_limit ON
    //   tbl_invoice_discounting.reference_no = tbl_buyer_required_limit.invRefNo

    //   LEFT JOIN tbl_document_details frameworkSignStatus ON
    //   tbl_buyer_required_limit.frameworkDoc = frameworkSignStatus.id

    //   LEFT JOIN tbl_document_details exhibitSignStatus ON
    //   tbl_buyer_required_limit.exhibitDoc = exhibitSignStatus.id

    //   LEFT JOIN tbl_document_details noaSignStatus ON
    //   tbl_buyer_required_limit.noaDoc = noaSignStatus.id

    //   WHERE tbl_invoice_discounting.reference_no = '${reqBody.invRefNo}' `

    //   dbRes = await call({ query }, 'makeQuery', 'get');
    //   let invoiceData = dbRes.message?.[0] || {}
    //   // invoice created at
    //   transactionHistory.push({
    //     action: `Invoice Application Applied `,
    //     dateTime: invoiceData.created_at,
    //     date: invoiceData.created_at ? moment(invoiceData.created_at).format("DD MMM, YYYY") : "NA",
    //     time: invoiceData.created_at ? moment(invoiceData.created_at).format("hh:mm a") : "NA"
    //   })
    //   if(invoiceData?.status/1 == 5){
    //     // invoice rejected at
    //     transactionHistory.push({
    //     action: `Invoice Application Rejected `,
    //     dateTime: invoiceData.rejectedOn,
    //     date: invoiceData.rejectedOn ? moment(invoiceData.rejectedOn).format("DD MMM, YYYY") : "NA",
    //     time: invoiceData.rejectedOn ? moment(invoiceData.rejectedOn).format("hh:mm a") : "NA"
    //     }) 
    //   }
    //   // adding invoice agreement statuses start
    //   else{
    //     // agreement sent log
    //     if(invoiceData?.frameworkDoc || invoiceData?.exhibitDoc || invoiceData?.noaDoc){
    //       transactionHistory.push({
    //         action: `Agreement Sent By Financier  `,
    //         dateTime: invoiceData.agreementSentOn,
    //         date: invoiceData.agreementSentOn ? moment(invoiceData.agreementSentOn).format("DD MMM, YYYY") : "NA",
    //         time: invoiceData.agreementSentOn ? moment(invoiceData.agreementSentOn).format("hh:mm a") : "NA"
    //         }) 
    //     }   
    //     // agreement sign by exporter log
    //     if(invoiceData?.frameworkExporterSign || invoiceData?.exhibitExporterSign || invoiceData?.noaExporterSign){
    //       transactionHistory.push({
    //         action: `Agreement Signed By Exporter `,
    //         dateTime: invoiceData.agreementSignByExporter,
    //         date: invoiceData.agreementSignByExporter ? moment(invoiceData.agreementSignByExporter).format("DD MMM, YYYY") : "NA",
    //         time: invoiceData.agreementSignByExporter ? moment(invoiceData.agreementSignByExporter).format("hh:mm a") : "NA"
    //         }) 
    //     } 
    //     // agreement sign by buyer log
    //     if(invoiceData?.frameworkBuyerSign || invoiceData?.exhibitBuyerSign || invoiceData?.noaBuyerSign){
    //       transactionHistory.push({
    //         action: `Agreement Signed By Buyer `,
    //         dateTime: invoiceData.agreementSignByBuyer,
    //         date: invoiceData.agreementSignByBuyer ? moment(invoiceData.agreementSignByBuyer).format("DD MMM, YYYY") : "NA",
    //         time: invoiceData.agreementSignByBuyer ? moment(invoiceData.agreementSignByBuyer).format("hh:mm a") : "NA"
    //         }) 
    //     } 
    //     // agreement sign by financier log
    //     if(invoiceData?.frameworkFinancierSign || invoiceData?.exhibitFinancierSign || invoiceData?.noaFinancierSign){
    //       transactionHistory.push({
    //         action: `Agreement Signed By Financier `,
    //         dateTime: invoiceData.agreementSignByFinancier,
    //         date: invoiceData.agreementSignByFinancier ? moment(invoiceData.agreementSignByFinancier).format("DD MMM, YYYY") : "NA",
    //         time: invoiceData.agreementSignByFinancier ? moment(invoiceData.agreementSignByFinancier).format("hh:mm a") : "NA"
    //         }) 
    //     }
    //   }
    //   // adding invoice agreement statuses end
    //   if(invoiceData?.status/1 == 3 || invoiceData?.status/1 == 4 || invoiceData?.status/1 == 6){
    //     // invoice approved at
    //     transactionHistory.push({
    //     action: `Invoice Application Approved `,
    //     dateTime: invoiceData.approvedOn,
    //     date: invoiceData.approvedOn ? moment(invoiceData.approvedOn).format("DD MMM, YYYY") : "NA",
    //     time: invoiceData.approvedOn ? moment(invoiceData.approvedOn).format("hh:mm a") : "NA"
    //     }) 
    //     if(invoiceData?.status/1 == 4 || invoiceData?.status/1 == 6){
    //       // invoice inprogress at
    //       transactionHistory.push({
    //         action: `Invoice Application Disbursement Inprogress `,
    //         dateTime: invoiceData.inprogressOn,
    //         date: invoiceData.inprogressOn ? moment(invoiceData.inprogressOn).format("DD MMM, YYYY") : "NA",
    //         time: invoiceData.inprogressOn ? moment(invoiceData.inprogressOn).format("hh:mm a") : "NA"
    //         })
    //       if(invoiceData?.status/1 == 4){
    //         // invoice disbursed at
    //         transactionHistory.push({
    //         action: `Invoice Application Disbursed `,
    //         dateTime: invoiceData.disbursedOn,
    //         date: invoiceData.disbursedOn ? moment(invoiceData.disbursedOn).format("DD MMM, YYYY") : "NA",
    //         time: invoiceData.disbursedOn ? moment(invoiceData.disbursedOn).format("hh:mm a") : "NA"
    //         })
    //       }
    //     }
    //   }
    // }
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
    console.log("in getTransactionHistoryForLC error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}