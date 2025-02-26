const { dbPool } = require("../../src/database/mysql")
const { call } = require("../../utils/codeHelper")
const { activeUserLogicDurationInWeeks, emailEnabledBanks, env, enabledFinanciersForLC, bccEmails, encryptData, environment, decryptData, karzaAPIKey, mongoConnectionString } = require("../../urlCostants");
const moment = require("moment");
const XLSX = require('xlsx');
const fs = require('fs');
const TTV = require("../../src/database/Models/TTVModel");
const CRMTasksLogs = require("../../src/database/Models/CRMTaskLogs");
const ExporterModel = require("../../src/database/Models/ExporterModel");
const TTVSummary = require("../../src/database/Models/TTVSummary");
const { assignTasksToSubAdmins } = require("../../src/cronjobs/TaskAssignments");
const { redisInstance } = require("../../iris_server/redis");
const ExporterModelV2 = require("../../src/database/Models/ExporterModelV2");
const TTVSummaryV2 = require("../../src/database/Models/TTVSummaryV2");
const BuyerModelV2 = require("../../src/database/Models/BuyerModelV2");
const CRMApplications = require("../../src/database/Models/CRMApplicationModel");
const CRMApplicationLogs = require("../../src/database/Models/CRMApplicationLogs");
const CRMFinanciers = require("../../src/database/Models/CRMFinancierModel");
const CRMFinLogs = require("../../src/database/Models/CRMFinancierLogs");
const { ObjectId, MongoClient, Db } = require("mongodb");
const { default: axios } = require("axios");
const TTVModelV2 = require("../../src/database/Models/TTVModelV2");
const { formatSqlQuery, mysqlTextParse, getCurrentTimeStamp, apiCallV2 } = require("../../iris_server/utils");
const ExporterComments = require("../../src/database/Models/ExporterCommentsModel");
const { sendMail } = require("../../utils/mailer");
const config = require("../../config");
const { fetchUserBuyerDetailsFromTTVData } = require('../../src/cronjobs/shipmentData');
const CRMFolder = require("../../src/database/Models/CRMFolder");
const { extractFullNames, designations, convertStringToList, getLatestDate, escapeRegExp, LCPurposeObject } = require("./utils");
const path = require("path");
const CRMTaskAssignment = require("../../src/database/Models/CRMTaskAssignment");
var nodemailer = require('nodemailer');
const { addUserInNetworkFunc } = require("../userNetworkController/addUserInNetwork");
const { LoginV2 } = require("../loginlogoutControllers/login");
const { default: mongoose } = require("mongoose");

const hsncolors = ['#FF6A6A','#76EEA9','#FFC062','#FF83AF','#AED8FF']
const statusArr  = [
  {
      "name": "Leads",
      "is_checked": true,
      "status": 1
  },
  {
      "name": "Task",
      "is_checked": true,
      "status": 0
  },
  {
      "name": "Not Intrested",
      "is_checked": true,
      "status": 2
  },
  {
      "name": "Lost",
      "is_checked": true,
      "status": 3
  },
  {
      "name": "Pending",
      "is_checked": true,
      "status": "Pending"
  }
]
const subadmins = [] //Add Production admins for which you want summary

function areArraysOfObjectsIdentical(array1, array2, key) {
  if (array1.length !== array2.length) {
    return false;
  }

  const getKeyedMap = (arr) => arr.reduce((acc, obj) => ({ ...acc, [obj[key]]: obj }), {});

  const keyedMap1 = getKeyedMap(array1);
  const keyedMap2 = getKeyedMap(array2);

  const keys1 = Object.keys(keyedMap1);
  const keys2 = Object.keys(keyedMap2);

  if (keys1.length !== keys2.length) {
    return false;
  }

  for (const key of keys1) {
    if (!keyedMap2[key] || JSON.stringify(keyedMap1[key]) !== JSON.stringify(keyedMap2[key])) {
      return false;
    }
  }

  return true;
}

function isStringInArrayOfObjects(arr, searchString) {
  for (const obj of arr) {
    for (const key in obj) {
      if (typeof obj[key] === 'string' && obj[key] === searchString) {
        return true;
      }
    }
  }
  return false;
}

function isArraySubsetOfAnother(array1, array2, key) {
  const set1 = new Set(array1.map((obj) => obj[key]));
  const set2 = new Set(array2.map((obj) => obj[key]));

  for (const value of set1) {
    if (!set2.has(value)) {
      return false;
    }
  }

  return true;
}

exports.getTasksForAdminFilters = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filterData = {}
    //
    filterData["Lead Status"] = {
      "accordianId": 'leadStatus',
      type: "checkbox",
      labelName: "name",
      data: [{name: "Active"}, {name: "Inactive"}, {name: "New User"},  {name: 'Agreement Not Sent'},
      // {name: "Quote Recieved"}, 
      // {name: "Quote Selected by exporter"}
    ]
    }
    //
    filterData["Application Status"] = {
      "accordianId": 'applicationStatus',
      type: "checkbox",
      labelName: "name",
      data: [{name: "Ongoing"}, {name: "Approved"}, {name: "Rejected"}]
    }
    //
    filterData["New User Type"] = {
      "accordianId": 'newUserType',
      type: "checkbox",
      labelName: "name",
      data: [{name: "Exporter"}, {name: "Channel Partner"}]
    }
    //
    filterData["Lead Assignment Status"] = {
      "accordianId": 'leadAssignmentStatus',
      type: "checkbox",
      labelName: "name",
      data: [{name: "Assigned"}, {name: "Unassigned"}]
    }
    //
    if(!reqBody.onlyShowForUserId){
      filterData["Lead Assigned To"] = {
        "accordianId": 'leadAssignedTo',
        type: "checkbox",
        labelName: "name"
      }
      let query = `SELECT tbl_user_details.contact_person AS name FROM tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user.id = tbl_user_details.tbl_user_id WHERE tbl_user.isSubUser = 1 AND tbl_user.type_id = 1 `
      let dbRes = await call({ query }, 'makeQuery', 'get');
      filterData["Lead Assigned To"]["data"] = dbRes.message
    }

    filterData["Company Name"] = {
      "accordianId": 'companyName',
      type: "checkbox",
      labelName: "name"
    }

    filterData["Contact No"] = {
      "accordianId": 'contactNo',
      type: "checkbox",
      labelName: "name"
    }

    filterData["Contact Person"] =  {
      "accordianId": 'contactPerson',
      type: "checkbox",
      labelName: "name"
    }
    let extraQuery = ""
    if(reqBody.onlyShowForUserId){
      extraQuery  = ` AND tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}'`
    }
    let query2 =  `SELECT
    tbd.company_name,
    tbd.contact_number,
    tbd.contact_person,
    tbl_user.modified_at as updated_at
    
    FROM tbl_user_details tbd

    LEFT JOIN tbl_user ON
    tbd.tbl_user_id = tbl_user.id

    LEFT JOIN (SELECT Count(*) AS buyers_count,
      user_id
      FROM   tbl_buyers_detail
      GROUP  BY tbl_buyers_detail.user_id) tb
      ON tb.user_id = tbd.tbl_user_id

    LEFT JOIN (SELECT Count(id) AS limit_count,
      userId
      FROM    tbl_buyer_required_limit
      GROUP  BY tbl_buyer_required_limit.userId) tl
      ON tl.userId = tbd.tbl_user_id

    LEFT JOIN  tbl_request_channel_partner ON
    tbl_user.id = tbl_request_channel_partner.user_id


    WHERE ((tbl_user.type_id = 19 AND ((tb.buyers_count IS NULL) OR (tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL))) OR   (tbl_user.type_id = 20 AND ((tbl_user.LeadAssignedTo IS NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4)) OR  (tbl_user.LeadAssignedTo IS NOT NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4))) ) ) ${extraQuery}
    GROUP BY tbd.tbl_user_id
    ORDER BY tbl_user.modified_at DESC
    `
    let query3 = `SELECT 
    tbl_user_details.company_name,
    tbl_user_details.contact_number,
    tbl_user_details.contact_person,
    view_tasks.updated_at
    FROM view_tasks 
      
    LEFT JOIN tbl_user_details ON
    tbl_user_details.tbl_user_id = view_tasks.userId

    LEFT JOIN tbl_user ON
    tbl_user.id = view_tasks.userId

    WHERE 1 ${extraQuery}
    `
    let dbRes2  = await call({query:query2},'makeQuery','get')
    let dbRes3  = await call({query:query3},'makeQuery','get')
    let dataToReturn = [...dbRes2.message, ...dbRes3.message]
    dataToReturn = dataToReturn.sort((a,b) => new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime() )   
    const uniqueCompany =  [...new Map(dataToReturn.map(item => [item["company_name"], {name : item.company_name}])).values()];
    const uniqueContactNo = [...new Map(dataToReturn.map(item => [item["contact_number"], {name : item.contact_number}])).values()];
    const uniqueContactPerson = [...new Map(dataToReturn.map(item => [item["contact_person"], {name : item.contact_person}])).values()];

    filterData["Company Name"]["data"] = uniqueCompany
    filterData["Contact No"]["data"] = uniqueContactNo
    filterData["Contact Person"]["data"] = uniqueContactPerson

    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("error in getTasksForAdminFilters", error);
    res.send({
      success: false,
      message: error
    })
  }
}


exports.getTasksForAdmin = async (req, res, next) => {
  try {
    let reqBody = req.body
    let query = ""
    let dbRes = {
      message:[],
      success:false
    }
    let dataToReturn = []

    let extraCondition = ""
    let extraConditionFor2ndQuery = ""
    let extraConditionFor1stQuery = ""
    let onlyWhereCondition = ""
    let newUserOnly = reqBody?.leadStatus?.includes("New User") || reqBody?.leadStatus?.includes("Agreement Not Sent") || reqBody.leadAssignmentStatus

    if(reqBody.onlyShowForUserId){
      extraCondition = ` AND (tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}'  OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.LeadAssignedTo IS NULL)`
    }
    if(reqBody.subadminIds){
      extraCondition += ` AND (tbl_user.LeadAssignedTo IN ('${reqBody.subadminIds?.join("','")}') )`
    
    }

    if(reqBody.companyName){
      extraConditionFor1stQuery += ` AND tbd.company_name IN (${reqBody.companyName.join(",")})`
      extraConditionFor2ndQuery += ` AND tbl_user_details.company_name IN (${reqBody.companyName.join(",")})`
    }

    if(reqBody.contactPerson){
      extraConditionFor1stQuery += ` AND tbd.contact_person IN (${reqBody.contactPerson.join(",")})`
      extraConditionFor2ndQuery += ` AND tbl_user_details.contact_person IN (${reqBody.contactPerson.join(",")})`
    }
    if(reqBody.contactNo){
      extraConditionFor1stQuery += ` AND tbd.contact_number IN (${reqBody.contactNo.join(",")})`
      extraConditionFor2ndQuery += ` AND tbl_user_details.contact_number IN (${reqBody.contactNo.join(",")})`
    }

    if(reqBody.search){
      extraConditionFor1stQuery += ` AND tbd.company_name LIKE '%${reqBody.search}%'`
      extraConditionFor2ndQuery += ` AND tbl_user_details.company_name LIKE '%${reqBody.search}%'`
    }

    if (reqBody.leadStatus) {
      let isActive = reqBody.leadStatus.includes("Active")
      let isInactive = reqBody.leadStatus.includes("Inactive")
      let agreementNotSent = reqBody?.leadStatus?.includes("Agreement Not Sent")
      if (isActive) {
        extraCondition += ` AND tbl_user.last_login_at >= DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) `
      }
      if(isInactive){
        extraCondition += ` AND (tbl_user.last_login_at IS NULL OR tbl_user.last_login_at < DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) ) `
      }
      if(agreementNotSent){
        onlyWhereCondition += ` AND (tbl_user.type_id = 20 AND ((tbl_user.LeadAssignedTo IS NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4)) OR  (tbl_user.LeadAssignedTo IS NOT NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4))) ) `
      }
    }

    if(reqBody?.newUserType?.includes("Exporter")){
      onlyWhereCondition += `  AND ( tbl_user.type_id = 19 AND ((tb.buyers_count IS NULL)  OR (tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL)))`
      extraConditionFor2ndQuery += ` AND tbl_user.type_id = 19 `
    }
    if(reqBody?.newUserType?.includes("Channel Partner")){
      onlyWhereCondition += ` AND (tbl_user.type_id = 20 AND ((tbl_user.LeadAssignedTo IS NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4)) OR  (tbl_user.LeadAssignedTo IS NOT NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4))) ) `
      extraConditionFor2ndQuery += ` AND tbl_user.type_id = 20 `
    }

    if(reqBody?.leadAssignmentStatus?.includes("Assigned")){
      onlyWhereCondition += ` AND (tbl_user.LeadAssignedTo IS NOT NULL)  AND  
      ((tbl_user.type_id = 19 AND ((tb.buyers_count IS NULL) OR (tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL)))
      OR  
      (tbl_user.type_id = 20 AND ((tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4 ))))`
      extraConditionFor2ndQuery += ` AND tbl_user.LeadAssignedTo IS NOT NULL `
    }
    if(reqBody?.leadAssignmentStatus?.includes("Unassigned")){
      onlyWhereCondition += ` AND (tbl_user.LeadAssignedTo IS  NULL)  AND  
      ((tbl_user.type_id = 19 AND ((tb.buyers_count IS NULL) OR (tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL)))
      OR  
      (tbl_user.type_id = 20 AND ((tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4 ))))`
      extraConditionFor2ndQuery += ` AND tbl_user.LeadAssignedTo IS NULL `
    }

    if(reqBody?.leadAssignedTo?.length){
      onlyWhereCondition += ` AND subAdminTblUserDetails.contact_person IN (${reqBody.leadAssignedTo.join(",")})   AND  
      (tbl_user.type_id = 19 AND ((tb.buyers_count IS NULL) OR (tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL)))
      OR  
      (tbl_user.type_id = 20 AND (tbl_user.LeadAssignedTo IS NULL AND (tbl_request_channel_partner.status != 3 AND 					tbl_request_channel_partner.status != 4 ))
       OR  (tbl_user.LeadAssignedTo IS NOT NULL AND (tbl_request_channel_partner.status != 3 AND 						 tbl_request_channel_partner.status != 4)))`
      extraConditionFor2ndQuery += ` AND subAdminTblUserDetails.contact_person IN (${reqBody.leadAssignedTo.join(",")}) `
    }

    if (reqBody.applicationStatus) {
      let isOngoing = reqBody.applicationStatus.includes("Ongoing")
      let isApproved = reqBody.applicationStatus.includes("Approved")
      let isRejected = reqBody.applicationStatus.includes("Rejected")
      if (isOngoing) {
        extraConditionFor2ndQuery += ` AND (tbl_buyer_required_lc_limit.financeStatus IN (0) OR tbl_invoice_discounting.status IN (1) ) `
      }
      if(isApproved){
        extraConditionFor2ndQuery += ` AND (tbl_buyer_required_lc_limit.financeStatus IN (1) OR tbl_invoice_discounting.status IN (3,4,6) ) `
      }
      if(isRejected){
        extraConditionFor2ndQuery += ` AND (tbl_buyer_required_lc_limit.financeStatus IN (2) OR tbl_invoice_discounting.status IN (5) )  `
      }
    }

    // Today onboarded users who didnt add any buyers
      let timeFilterQuery = " 1 "
      if(reqBody.isTodaysTasks && reqBody.dateRangeFilter){
        timeFilterQuery = ` tbd.created_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}' `
      }
      query = `SELECT
      tbd.company_name,
      tbd.phone_code,
      tbd.contact_number,
      tbd.contact_person,
      tbd.name_title,
      tbl_user.type_id,
      tbl_user.status,
      tbl_user.LeadAssignedTo,
      tbl_user.id as userId,
      tbl_user.leadNote,
      tbd.email_id,
      subAdminTblUserDetails.contact_person AS subAdminContactPersonName,
      tb.buyers_count,
      tbl_user.modified_at as updated_at,
      tl.limit_count,
      tbl_request_channel_partner.status as CPStatus
      
      FROM tbl_user_details tbd
  
      LEFT JOIN tbl_user ON
      tbd.tbl_user_id = tbl_user.id
  
      LEFT JOIN (SELECT Count(*) AS buyers_count,
        user_id
        FROM   tbl_buyers_detail
        GROUP  BY tbl_buyers_detail.user_id) tb
        ON tb.user_id = tbd.tbl_user_id

      LEFT JOIN (SELECT Count(id) AS limit_count,
        userId
        FROM    tbl_buyer_required_limit
        GROUP  BY tbl_buyer_required_limit.userId) tl
        ON tl.userId = tbd.tbl_user_id

  
      LEFT JOIN tbl_countries supplierCountry ON
      tbd.country_code = supplierCountry.sortname

      LEFT JOIN  tbl_request_channel_partner ON
      tbl_user.id = tbl_request_channel_partner.user_id

      LEFT JOIN tbl_user subAdminTblUser ON
      tbl_user.LeadAssignedTo = subAdminTblUser.id

      LEFT JOIN tbl_user_details subAdminTblUserDetails ON
      subAdminTblUser.id = subAdminTblUserDetails.tbl_user_id
  
      WHERE 
      ${timeFilterQuery}
      ${onlyWhereCondition ? onlyWhereCondition : `  AND (tbl_user.status = 1  AND  (tbl_user.type_id = 19 AND ((tb.buyers_count IS NULL) OR (tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL))) OR   (tbl_user.type_id = 20 AND ((tbl_user.LeadAssignedTo IS NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4)) OR  (tbl_user.LeadAssignedTo IS NOT NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4))) ) ) `}
      ${extraCondition} ${extraConditionFor1stQuery}
      GROUP BY tbd.tbl_user_id
      ORDER BY tbl_user.modified_at DESC
      `
      dbRes = await call({ query }, 'makeQuery', 'get');
      dataToReturn = dataToReturn.concat(dbRes.message)
      

    query = `SELECT subq.*,
    tbl_user_details.company_name,
      tbl_user_details.phone_code,
      tbl_user_details.contact_number,
      tbl_user_details.contact_person,
      tbl_user_details.name_title,
      tbl_user.type_id,
      tbl_user.status,
      tbl_user.LeadAssignedTo,
      tbl_user.id as userId,
      tbl_user_details.email_id,
      tbl_buyer_required_limit.leadNote AS invoiceLimitLeadNote,
      tbl_buyer_required_lc_limit.leadNote lcLimitLeadNote,
      subAdminTblUserDetails.contact_person AS subAdminContactPersonName,
      tbl_buyers_detail.buyerCountry
      FROM (
        SELECT *, (CASE 
              WHEN invRefNo = 'lc_discounting' THEN  'lc_discounting' 
              WHEN invRefNo = 'lc_confirmation' THEN 'lc_confirmation'
              WHEN invRefNo = 'sblc' THEN 'sblc'
              ELSE 'invoice_discounting'
              END) as finance_type 
        FROM view_tasks
      ) subq 
      
      LEFT JOIN tbl_user_details ON
      tbl_user_details.tbl_user_id = subq.userId
  
      LEFT JOIN tbl_user ON 
      tbl_user.id = subq.userId 
      
      LEFT JOIN tbl_user subAdminTblUser ON
      tbl_user.LeadAssignedTo = subAdminTblUser.id

      LEFT JOIN tbl_user_details subAdminTblUserDetails ON
      subAdminTblUser.id = subAdminTblUserDetails.tbl_user_id 

      LEFT JOIN tbl_buyer_required_limit ON
      subq.tblId = tbl_buyer_required_limit.id

      LEFT JOIN tbl_buyer_required_lc_limit ON
      subq.tblId = tbl_buyer_required_lc_limit.id

      LEFT JOIN tbl_invoice_discounting ON
      tbl_buyer_required_limit.invRefNo = tbl_invoice_discounting.reference_no

      
      LEFT JOIN tbl_buyers_detail ON
      tbl_buyers_detail.id  = subq.buyerId

      WHERE 1 AND tbl_user.status = 1  ${extraCondition} ${extraConditionFor2ndQuery} `
     
      
      if(reqBody.isTodaysTasks && reqBody.dateRangeFilter){
        query += ` AND subq.updated_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}'  `
      }

      query += ' ORDER BY subq.updated_at DESC'

      //WHERE subq.updated_at BETWEEN '${reqBody.dateRangeFilter[0]}' AND '${reqBody.dateRangeFilter[1]}'`
      // if(reqBody.resultPerPage && reqBody.currentPage){
      //   var perPageString = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage}`;
      //   query += perPageString
  
      // }
    let dbRes2 = newUserOnly ? {message: []} : await call({ query }, 'makeQuery', 'get');
    dataToReturn = dataToReturn.concat(dbRes2.message)
    for(let i = 0; i<= dataToReturn.length - 1 ; i++){
      let element = dataToReturn[i]
      let queryy = ` 
      SELECT 
        tbl_user_tasks_logs.LOG_TYPE AS LastEventType,
        tbl_user_tasks_logs.CREATED_AT AS LastEventTime,
        tbl_user_tasks_logs.REMARK AS LastNote
      FROM 
        tbl_user_tasks_logs WHERE EXPORTER_CODE = '${element.userId}' ORDER BY CREATED_AT DESC LIMIT 1`
      
      const  userLogsRes = await call({query:queryy},'makeQuery','get')
      let userLogsObj = userLogsRes.message?.[0] || {}
      element.LastEventType = userLogsObj.LastEventType
      element.LastEventTime = userLogsObj.LastEventTime
      element.LastNote = userLogsObj.LastNote
      element.updated_at = getLatestDate(element.updated_at, userLogsObj.LastEventTime);
    }

    if(reqBody.sortCompanyName){
      if(reqBody.sortCompanyName === 'DESC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.company_name > b.company_name)
            return -1;
          if (a.company_name < b.company_name)
            return 1;
          return 0;
        })   
      }else if(reqBody.sortCompanyName === 'ASC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.company_name < b.company_name)
            return -1;
          if (a.company_name > b.company_name)
            return 1;
          return 0;
        })    
      }
    }else if(reqBody.sortContactPerson){
      if(reqBody.sortContactPerson === 'DESC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.contact_person > b.contact_person)
            return -1;
          if (a.contact_person < b.contact_person)
            return 1;
          return 0;
        })   
      }else if(reqBody.sortContactPerson === 'ASC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.contact_person < b.contact_person)
            return -1;
          if (a.contact_person > b.contact_person)
            return 1;
          return 0;
        })    
      }
    }else if(reqBody.sortLeadAssignedTo){
      if(reqBody.sortLeadAssignedTo === 'DESC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.subAdminContactPersonName > b.subAdminContactPersonName)
            return -1;
          if (a.subAdminContactPersonName < b.subAdminContactPersonName)
            return 1;
          return 0;
        })   
      }else if(reqBody.sortLeadAssignedTo === 'ASC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.subAdminContactPersonName < b.subAdminContactPersonName)
            return -1;
          if (a.subAdminContactPersonName > b.subAdminContactPersonName)
            return 1;
          return 0;
        })    
      }
    }else if(reqBody.sortByPeriod){
      if(reqBody.sortByPeriod === 'DESC'){
        dataToReturn = dataToReturn.sort((a,b) => new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime() )   
      }else if(reqBody.sortByPeriod === 'ASC'){
        dataToReturn = dataToReturn.sort((a,b) => new Date(a.updated_at).getTime() - new Date(b.updated_at).getTime() )   
      }
    }
    else{
      dataToReturn = dataToReturn.sort((a,b) => new Date(a.updated_at).getTime() - new Date(b.updated_at).getTime() )   
    }
    let dCount =  dataToReturn.length
    if(reqBody.resultPerPage && reqBody.currentPage){
      const startindex = (reqBody.currentPage - 1) * reqBody.resultPerPage;
      const endIndex = startindex + reqBody.resultPerPage;
      dataToReturn  = dataToReturn.slice(startindex,endIndex)
    }

    res.send({
      success: true,
      message: {
        message:dataToReturn,
        totalCount : dCount
      }
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

exports.updateLeadAssignedTo = async (req,res) => {
  try{
    const query = `UPDATE tbl_user SET LeadAssignedTo='${req.body.leadAssignedName}' WHERE id=${req.body.userId}`
    await dbPool.query(query)
    res.send({
      success:true,
      message:'Lead Updated Succesfully'
    })
  }catch(e){
    res.send({
      success:true,
      message:'Failed to update lead.'
    })
  }
}

exports.addNoteForLead = async (req,res) => {
  try{
    let reqBody = req.body
    let query = ""
    if(reqBody.userTblId){
      query = `UPDATE tbl_user SET leadNote = '${reqBody.leadNote}' WHERE id = '${reqBody.userTblId}' `
    }
    if(reqBody.invoiceLimitId){
      query = `UPDATE tbl_buyer_required_limit SET leadNote = '${reqBody.leadNote}' WHERE id = '${reqBody.invoiceLimitId}' `
    }
    if(reqBody.lcNo){
      query = `UPDATE tbl_buyer_required_lc_limit SET leadNote = '${reqBody.leadNote}' WHERE lcNo = '${reqBody.lcNo}' `
    }
    await dbPool.query(query)
    res.send({
      success:true,
      message:'Note Saved'
    })
  }catch(e){
    res.send({
      success:false,
      message:'Failed to update Note.'
    })
  }
}

exports.getTaskManagerGraphData = async (req,res) => {
  try{
    let reqBody = req.body
    let todayDateObj = moment()
    let query = ""
    let dbRes = null
    let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
    let response = {
      //1
      "activeUserApplicationSummary": {
        "Finance Limit": {},
        "Quote": {},
        "Termsheet/Contract": {},
        "Finance": {},
        "Agreement": {},
        "Approved": {}
      },
      "inactiveUserDayWiseSummary": {
        "15 Days": {},
        "30 Days": {},
        "45 Days": {},
        "60 Days": {},
        "75 Days": {}
      }
    }
    // Active User Application Stages
    let subQuery = ` BETWEEN '${reqBody.applicationStageFrom}' AND '${reqBody.applicationStageTo}' `
    
    // Applied but not received quote invoice
    query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.buyers_credit IS NULL AND tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Finance Limit"]["invoice"] = dbRes.message.length
    // Applied but not received quote lc
    query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.financierQuotes IS NULL AND tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Finance Limit"]["lc"] = dbRes.message.length

    // Applied and received quote but not selected financier invoice
    query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.buyers_credit IS NOT NULL AND tbl_buyer_required_limit.selectedFinancier IS NULL AND
    tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Quote"]["invoice"] = dbRes.message.length
    // Applied and received quote but not selected financier lc
    query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.financierQuotes IS NOT NULL AND tbl_buyer_required_lc_limit.selectedFinancier IS NULL AND
    tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Quote"]["lc"] = dbRes.message.length

    // Applied and received termsheet but not applied for finance invoice
    query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.termSheet IS NOT NULL AND tbl_buyer_required_limit.invRefNo IS NULL AND
    tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Termsheet/Contract"]["invoice"] = dbRes.message.length
    // Applied and received termsheet but not applied for finance lc
    query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.reqLetterOfConfirmation IS NOT NULL AND tbl_buyer_required_lc_limit.invRefNo IS NULL AND
    tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Termsheet/Contract"]["lc"] = dbRes.message.length

    // Applied for finance invoice
    query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.invRefNo IS NOT NULL AND tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Finance"]["invoice"] = dbRes.message.length
    // Applied for finance lc
    query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.invRefNo IS NOT NULL AND tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Finance"]["lc"] = dbRes.message.length

    // Agreement sent invoice
    query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE tbl_buyer_required_limit.updatedAt ${subQuery} AND
    (tbl_buyer_required_limit.frameworkDoc IS NOT NULL OR tbl_buyer_required_limit.exhibitDoc IS NOT NULL OR tbl_buyer_required_limit.noaDoc IS NOT NULL) AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Agreement"]["invoice"] = dbRes.message.length
    
    // Approved finance for invoice
    query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_invoice_discounting ON
    tbl_buyer_required_limit.buyerId = tbl_invoice_discounting.buyer_id
    WHERE
    tbl_invoice_discounting.status = 3 AND tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Approved"]["invoice"] = dbRes.message.length
    // Approved finance for lc
    query = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.financeStatus = 1 AND tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    response["activeUserApplicationSummary"]["Approved"]["lc"] = dbRes.message.length


    let userQuery = `SELECT 
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 1 ELSE 0 END) AS '15days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 1 ELSE 0 END) AS '30days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 45 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS '45days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 60 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 45 DAY) THEN 1 ELSE 0 END) AS '60days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 75 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 60 DAY) THEN 1 ELSE 0 END) AS '75days',
        SUM(CASE WHEN last_login_at < DATE_SUB(NOW(), INTERVAL 75 DAY) OR last_login_at IS  NULL THEN 1 ELSE 0 END) AS '75days+'
      FROM tbl_user
      WHERE type_id = 19`

      let finQuery = `SELECT 
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 1 ELSE 0 END) AS '15days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 1 ELSE 0 END) AS '30days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 45 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS '45days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 60 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 45 DAY) THEN 1 ELSE 0 END) AS '60days',
        SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 75 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 60 DAY) THEN 1 ELSE 0 END) AS '75days',
        SUM(CASE WHEN last_login_at < DATE_SUB(NOW(), INTERVAL 75 DAY) OR last_login_at IS  NULL THEN 1 ELSE 0 END) AS '75days+'
      FROM tbl_user
      WHERE type_id = 8`

    let cpQuery = `SELECT 
    SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 1 ELSE 0 END) AS '15days',
    SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 1 ELSE 0 END) AS '30days',
    SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 45 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS '45days',
    SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 60 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 45 DAY) THEN 1 ELSE 0 END) AS '60days',
    SUM(CASE WHEN last_login_at >= DATE_SUB(NOW(), INTERVAL 75 DAY) AND last_login_at < DATE_SUB(NOW(), INTERVAL 60 DAY) THEN 1 ELSE 0 END) AS '75days',
    SUM(CASE WHEN last_login_at < DATE_SUB(NOW(), INTERVAL 75 DAY) OR last_login_at IS  NULL THEN 1 ELSE 0 END) AS '75days+'
      FROM tbl_user
      WHERE type_id = 20`
    const userRes = await call({query:userQuery},'makeQuery','get')
    const cpRes = await call({query:cpQuery},'makeQuery','get')
    const finRes = await call({query:finQuery},'makeQuery','get')

    let userCounts = userRes.message[0]
    let cpCounts =cpRes.message[0]
    let finCounts = finRes.message[0]

    for (let index = 0; index < Object.keys(response["inactiveUserDayWiseSummary"]).length; index++) {
      let item = Object.keys(response["inactiveUserDayWiseSummary"])[index]
      let days = item.split("Days")[0]/1
      if(days == "75"){
        response["inactiveUserDayWiseSummary"][days + " Days"]["exporter"] = parseInt(userCounts[`${days}days`])  + parseInt(userCounts[`${days}days+`]) 
        response["inactiveUserDayWiseSummary"][days + " Days"]["importer"] = 0
        response["inactiveUserDayWiseSummary"][days + " Days"]["channelPartner"] = parseInt(cpCounts[`${days}days`]) +  parseInt(cpCounts[`${days}days+`])
        response["inactiveUserDayWiseSummary"][days + " Days"]["financers"] = parseInt(finCounts[`${days}days`]) +  parseInt(finCounts[`${days}days+`])
      }else{
        response["inactiveUserDayWiseSummary"][days + " Days"]["exporter"] = parseInt(userCounts[`${days}days`])
        response["inactiveUserDayWiseSummary"][days + " Days"]["importer"] = 0
        response["inactiveUserDayWiseSummary"][days + " Days"]["channelPartner"] =parseInt(cpCounts[`${days}days`])
        response["inactiveUserDayWiseSummary"][days + " Days"]["financers"] =parseInt(finCounts[`${days}days`])
      }
    }

    res.send({
      success:true,
      message: response
    })
  }catch(e){
    console.log("error in getTaskManagerGraphData", e);
    res.send({
      success:false,
      message:'Failed to fetch data'
    })
  }
}

exports.getTasksStatsForAdmin = async (req, res, next) => {
  try {
    let query = ""
    let dbRes = {
      message:[],
      success:false
    }
    let dbRes2 = {
      message:[],
      success:false
    }
    let extraCondition = ""
    if(req.body.onlyShowForUserId){
      extraCondition = ` AND (tbl_user.LeadAssignedTo = '${req.body.onlyShowForUserId}' OR tbl_user.LeadAssignedTo IS NULL)`
    }
    
    query = `SELECT
      COUNT(tbl_user.id) AS total_users,
      ( CASE
          WHEN tbl_user.type_id = 19 THEN 'Exporter/Importer'
          WHEN tbl_user.type_id = 20 THEN 'Channel Partner'
          ELSE tbl_user.type_id
        end ) AS user_type,
       COUNT(CASE
              WHEN tbl_user.type_id = 20
                   AND tbl_request_channel_partner.status != 3 THEN 1
            end)   AS agreement_pending
      
      FROM tbl_user_details tbd
  
      LEFT JOIN tbl_user ON
      tbd.tbl_user_id = tbl_user.id
  
      LEFT JOIN (SELECT Count(*) AS buyers_count,
        user_id
        FROM   tbl_buyers_detail
        GROUP  BY tbl_buyers_detail.user_id) tb
        ON tb.user_id = tbd.tbl_user_id

      LEFT JOIN (SELECT Count(id) AS limit_count,
        userId
        FROM    tbl_buyer_required_limit
        GROUP  BY tbl_buyer_required_limit.userId) tl
        ON tl.userId = tbd.tbl_user_id

      LEFT JOIN  tbl_request_channel_partner ON
      tbl_user.id = tbl_request_channel_partner.user_id
        
      WHERE ((tbl_user.type_id = 19 AND ((tb.buyers_count IS NULL) OR (tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL))) OR   (tbl_user.type_id = 20 AND ( (tbl_user.LeadAssignedTo IS NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4) ) OR  (tbl_user.LeadAssignedTo IS NOT NULL AND (tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4))) )) ${extraCondition}    

      GROUP BY tbl_user.type_id`
      let query2 = `
      SELECT
    COUNT(
        CASE WHEN tbl_user.LeadAssignedTo IS NOT NULL THEN 1
    END
) AS lead_assigned,
COUNT(
    CASE WHEN tbl_user.LeadAssignedTo IS NULL THEN 1
END
) AS lead_not_assigned
FROM
    tbl_user_details tbd
LEFT JOIN tbl_user ON tbd.tbl_user_id = tbl_user.id
LEFT JOIN tbl_countries supplierCountry ON
    tbd.country_code = supplierCountry.sortname
LEFT JOIN(
    SELECT
        COUNT(*) AS buyers_count,
        user_id
    FROM
        tbl_buyers_detail
    GROUP BY
        tbl_buyers_detail.user_id
) tb
ON
    tb.user_id = tbd.tbl_user_id
LEFT JOIN(
    SELECT
        COUNT(id) AS limit_count,
        userId
    FROM
        tbl_buyer_required_limit
    GROUP BY
        tbl_buyer_required_limit.userId
) tl
ON
    tl.userId = tbd.tbl_user_id
LEFT JOIN tbl_request_channel_partner ON tbl_user.id = tbl_request_channel_partner.user_id
LEFT JOIN tbl_user subAdminTblUser ON
    tbl_user.LeadAssignedTo = subAdminTblUser.id
LEFT JOIN tbl_user_details subAdminTblUserDetails ON
    subAdminTblUser.id = subAdminTblUserDetails.tbl_user_id
WHERE
    1 ${extraCondition} AND(
        (
            tbl_user.type_id = 19 AND(
                (tb.buyers_count IS NULL) OR(
                    tb.buyers_count IS NOT NULL AND tl.limit_count IS NULL
                )
            )
        ) OR(
            tbl_user.type_id = 20 AND(
                (
                    tbl_request_channel_partner.status != 3 AND tbl_request_channel_partner.status != 4
                )
            )
        )
    )
      `
      dbRes = await call({ query }, 'makeQuery', 'get')
      dbRes2 = await call({query:query2},'makeQuery','get')
      let dataObj = {}
      let result = dbRes.message
      let result2 = dbRes2.message
      if (result.length) {
        dataObj["newUsersCount"] = result[0]?.total_users + result[1]?.total_users
        dataObj["impexpCount"] = result[0]?.total_users
        dataObj["CPCount"] = result[1]?.total_users
        dataObj["leadsAssignedCount"] = result2[0]?.lead_assigned
        dataObj["leadsNotAssignedCount"] = result2[0]?.lead_not_assigned
        dataObj["AgreementPending"] = result[0]?.agreement_pending + result[1]?.agreement_pending
      }
      res.send({
        success: true,
        message: dataObj
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

exports.getEnquiryStats = async (req,res) => {
  try{
    const result = await getEnquiryStatsFunc(req.body)
    res.send(result)
  }catch(e){
    console.log('error in getEnquiryStats',e)
    res.send(e)
  }
}

const getEnquiryStatsFunc = ({userId,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let generalquery = `SELECT COUNT(*) as general_counts FROM tbl_inquiry_from_website WHERE (productType !='LC Discounting' AND productType !='Invoice Discounting' AND productType !='LC Confirmation')`
      let quotesQuery = `SELECT COUNT(*) as quotes_count FROM tbl_inquiry_from_website WHERE (productType='LC Discounting' OR productType='Invoice Discounting' OR productType='LC Confirmation')`
      let leadsQuery = `SELECT COUNT(*) as leads_count FROM tbl_inquiry_from_website WHERE status=1`
      let leadsQueryConverted = `SELECT * FROM tbl_inquiry_from_website WHERE status=1`
      let leadLostQuery = `SELECT COUNT(*) as leadsLost_count FROM tbl_inquiry_from_website WHERE status=2`
      if(userId){
        let leadAssignedToQuery = ` AND LeadAssigedTo = ${userId}`
        generalquery += leadAssignedToQuery
        quotesQuery += leadAssignedToQuery
        leadsQuery += leadAssignedToQuery
        leadLostQuery += leadAssignedToQuery
        leadsQueryConverted += leadAssignedToQuery
      }
      if(onlyShowForUserId){
        let leadAssignedToQuery = ` AND LeadAssigedTo = ${onlyShowForUserId}`
        generalquery += leadAssignedToQuery
        quotesQuery += leadAssignedToQuery
        leadsQuery += leadAssignedToQuery
        leadLostQuery += leadAssignedToQuery
        leadsQueryConverted += leadAssignedToQuery
      }
      const generalRes = await call({query:generalquery}, 'makeQuery','get')
      const quotesRes = await call({query:quotesQuery}, 'makeQuery','get')
      const leadsRes = await call({query:leadsQuery}, 'makeQuery','get')
      const leadLostRes = await call({query:leadLostQuery}, 'makeQuery','get')
      const leadconverted = await call({query:leadsQueryConverted}, 'makeQuery','get')
      let generalCount = generalRes?.message[0]?.general_counts
      let quotesCount = quotesRes?.message[0]?.quotes_count
      const resObj = {
        generalCount,
        quotesCount ,
        totalCount :  generalCount + quotesCount,
        leadsCount : leadsRes?.message[0]?.leads_count,
        leadsLostCount : leadLostRes?.message[0]?.leadsLost_count ,
        assignedLead: leadconverted?.message
      }
      resolve({
        success:true,
        message:resObj
      })
    }catch(e){
      console.log('Sunbdsaadssad',e)
      reject({
        success:false,
        message:{
          generalCount:0,
          quotesCount:0 ,
          totalCount :  0 ,
          leadsCount : 0 ,
          leadsLostCount : 0  
        }
      })
    }
  })
}

exports.getEnquiryList = async (req,res) => {
  try{
    const result = await getEnquiryListFunc(req.body)
    res.send(result)
  }catch(e){
    console.log('error in getEnquiryStats',e)
    res.send(e)
  }
}

const getEnquiryListFunc = ({userId,resultPerPage,currentPage,search,enquiryType,LeadStatus,leadAssignedTo,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let query = `SELECT tbl_inquiry_from_website.*, tbl_user_details.contact_person as subAdminContactPersonName,    
      tbl_enquiry_tasks_logs.LOG_TYPE AS LastEventType,
      tbl_enquiry_tasks_logs.CREATED_AT AS LastEventTime,
      tbl_enquiry_tasks_logs.REMARK AS LastNote 
      FROM tbl_inquiry_from_website 
        LEFT JOIN tbl_user_details ON
        tbl_user_details.tbl_user_id = tbl_inquiry_from_website.LeadAssigedTo 
        
        LEFT JOIN tbl_enquiry_tasks_logs ON 
        tbl_inquiry_from_website.id = tbl_enquiry_tasks_logs.EXPORTER_CODE
        WHERE 1
      `
      let CountQuery = `SELECT COUNT(tbl_inquiry_from_website.id) as total_count  FROM tbl_inquiry_from_website 
        LEFT JOIN tbl_user_details ON
        tbl_user_details.tbl_user_id = tbl_inquiry_from_website.LeadAssigedTo WHERE 1
      `

      if(userId){
        let leadAssignedToQuery = ` AND tbl_inquiry_from_website.LeadAssigedTo = ${userId}`
        query += leadAssignedToQuery
        CountQuery += leadAssignedToQuery
      }
      if(onlyShowForUserId){
        let onlyShowForUserIdQuery = ` AND tbl_inquiry_from_website.LeadAssigedTo = ${onlyShowForUserId}`
        query += onlyShowForUserIdQuery
        CountQuery += onlyShowForUserIdQuery
      }
      if(search){
        query += ` AND tbl_inquiry_from_website.beneficiaryName LIKE '%${search}%'`
        CountQuery += ` AND tbl_inquiry_from_website.beneficiaryName LIKE '%${search}%'`
      }
      if(enquiryType?.length == 1){
        let extraQuery = ``
        if(enquiryType[0] === "'General Enquiry'"){
          extraQuery = ` AND (tbl_inquiry_from_website.productType !='LC Discounting' AND tbl_inquiry_from_website.productType !='Invoice Discounting' AND tbl_inquiry_from_website.productType !='LC Confirmation')`
        }
        if(enquiryType[0] === "'Quote Enquiry'"){
          extraQuery = ` AND (tbl_inquiry_from_website.productType ='LC Discounting' OR tbl_inquiry_from_website.productType = 'Invoice Discounting' OR tbl_inquiry_from_website.productType ='LC Confirmation')`
        }
        query += extraQuery
        CountQuery += extraQuery
      }

      if(LeadStatus?.length == 1){
        let extraQuery = ``
        if(LeadStatus[0] === "'Lead Converted'"){
          extraQuery = ` AND (tbl_inquiry_from_website.status = 1 )`
        }
        if(LeadStatus[0] === "'Lead Lost'"){
          extraQuery = ` AND (tbl_inquiry_from_website.status = 2 )`
        }
        query += extraQuery
        CountQuery += extraQuery
      }else if(LeadStatus?.length == 2){
        let extraQuery = ` AND (tbl_inquiry_from_website.status = 1 OR tbl_inquiry_from_website.status = 2)`
        query += extraQuery
        CountQuery += extraQuery
      }

      if(leadAssignedTo?.length){
        let extraCondition = ` AND tbl_user_details.contact_person IN (${leadAssignedTo.join(",")}) `
        query += extraCondition
        CountQuery += extraCondition
      }

      query += ` GROUP BY tbl_inquiry_from_website.id`
      query += ` ORDER BY tbl_inquiry_from_website.createdAt DESC`
      if (resultPerPage && currentPage) {
        var perPageString = ` LIMIT ${resultPerPage} OFFSET ${(currentPage - 1) * resultPerPage}`;
        query += perPageString
      }
      console.log('Qieryyuuu',query);
      const dbRes = await call({query},'makeQuery','get')
      const dbCountQuery = await call({query:CountQuery},'makeQuery','get')
      resolve({
        success:true,
        message:{
          message:dbRes.message,
          total_count : dbCountQuery?.message[0]?.total_count
        }
      })
    }catch(e){
      console.log('Sunbdsaadssad',e)
      reject({
        success:false,
        message: {
          message:[],
          total_count:0
        }
      })
    }
  })
}

exports.updateEnquiryLeadAssignedTo = async (req,res) => {
  try{
    const query = `UPDATE tbl_inquiry_from_website SET LeadAssigedTo='${req.body.leadAssignedName}' WHERE id=${req.body.id}`
    await dbPool.query(query)
    res.send({
      success:true,
      message:'Lead Updated Succesfully'
    })
  }catch(e){
    res.send({
      success:true,
      message:'Failed to update lead.'
    })
  }
}


exports.getEnquiryAdminFilters = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filterData = {}
    //
    filterData["Enquiry Type"] = {
      "accordianId": 'enquiryType',
      type: "checkbox",
      labelName: "name",
      data: [{name: "General Enquiry"}, {name: "Quote Enquiry"}]
    }
    //
    filterData["Lead Status"] = {
      "accordianId": 'LeadStatus',
      type: "checkbox",
      labelName: "name",
      data: [{name: "Lead Converted"}, {name: "Lead Lost"}]
    }
    filterData["Lead Assigned To"] = {
      "accordianId": 'leadAssignedTo',
      type: "checkbox",
      labelName: "name"
    }
    let query = `SELECT tbl_user_details.contact_person AS name FROM tbl_user 
    LEFT JOIN tbl_user_details ON tbl_user.id = tbl_user_details.tbl_user_id WHERE tbl_user.isSubUser = 1 AND tbl_user.type_id = 1 `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    filterData["Lead Assigned To"]["data"] = dbRes.message
    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("error in getEnquiryAdminFilters", error);
    res.send({
      success: false,
      message: error
    })
  }
}


exports.addNoteForEnquiry = async (req,res) => {
  try{
    let reqBody = req.body
    let query = `UPDATE tbl_inquiry_from_website SET adminNote = '${formatSqlQuery(reqBody.adminNote)}' WHERE id = ${reqBody.id}`
    await dbPool.query(query)
    res.send({
      success:true,
      message:'Note Saved'
    })
  }catch(e){
    console.log('error in addNoteForEnquiry',e)
    res.send({
      success:false,
      message:'Failed to update Note.'
    })
  }
}

exports.addAdminTasks = async (req,res) => {
  try{
    const result = await addAdminTasksFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const addAdminTasksFunc = ({ taskDate,HS_CODE }, reqFiles) => {
  console.log("called addadmin")
  return new Promise(async(resolve, reject) => {
    try {
      let filepath = ''
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
        const result = [];
        let currentObj = {};
        let not_added = []
        for (let i = 0; i <= data.length - 1; i++) {
          let obj = data[i]
          if (obj) {
            if (obj["EXPORTER_CODE"]) {
              if (Object.keys(currentObj).length) {
                result.push(currentObj);
              }
              const newObj = {
                EXPORTER_CODE: obj.EXPORTER_CODE,
                EXPORTER_NAME: obj.EXPORTER_NAME,
                EXPORTER_ADDRESS: obj.EXPORTER_ADDRESS,
                EXPORTER_CITY: obj.EXPORTER_CITY,
                TOTAL_BUYERS: obj.TOTAL_BUYERS,
                // FOB: obj.FOB,
                // FOB_IN_MILLION: obj['FOB (in million $)'],
                HS_CODE: obj['HS Code'],
                //TASK_DATE : taskDate
              }
              newObj["EXTRA_DETAILS"] = [{
                'Department': obj.Department,
                'GST/ Establishment Number': obj['GST/ Establishment Number'],
                'Contact Number': obj['Contact Number'],
                'DIN': obj['DIN'],
                'Contact Number': obj['Contact Number'],
                'Email ID': obj['Email ID']
              }]
              currentObj = { ...newObj };
              delete obj['Department']
              delete obj['GST/ Establishment Number']
              delete obj['Contact Number']
              delete obj['DIN']
              delete obj['Contact Number']
              delete obj['Email ID']
            } else if(obj["EXPORTER_NAME"]){
              const exporterinfo =await ExporterModel.find({EXPORTER_NAME : {
                $regex : new RegExp(obj.EXPORTER_NAME),
                $options: 'i'
              }})
              let expObj = exporterinfo?.[0]
              if(expObj){
                if (Object.keys(currentObj).length) {
                  result.push(currentObj);
                }
                const newObj = {
                  EXPORTER_CODE: expObj.EXPORTER_CODE,
                  EXPORTER_NAME: expObj.EXPORTER_NAME,
                  EXPORTER_ADDRESS: expObj.EXPORTER_ADDRESS,
                  EXPORTER_CITY: expObj.EXPORTER_CITY,
                  TOTAL_BUYERS: expObj.TOTAL_BUYERS,
                  // FOB: obj.FOB,
                  // FOB_IN_MILLION: obj['FOB (in million $)'],
                  //HS_CODE: obj['HS Code'],
                  //TASK_DATE : taskDate
                }
                newObj["EXTRA_DETAILS"] = [{
                  'Department': obj.Department,
                  'GST/ Establishment Number': obj['GST/ Establishment Number'],
                  'Contact Number': obj['Contact Number'],
                  'DIN': obj['DIN'],
                  'Contact Number': obj['Contact Number'],
                  'Email ID': obj['Email ID']
                }]
                currentObj = { ...newObj };
                delete expObj['Department']
                delete expObj['GST/ Establishment Number']
                delete expObj['Contact Number']
                delete expObj['DIN']
                delete expObj['Contact Number']
                delete expObj['Email ID']
                console.log('exportereinfgffffoo',currentObj);
              }else{
                not_added.push(obj.EXPORTER_NAME)
              }
            }  else {
              currentObj["EXTRA_DETAILS"] = currentObj.EXTRA_DETAILS.concat(obj)
              //get the exporter code
            }
          }
        }
        result.push(currentObj);
        for(let i=0; i<=result.length - 1; i++){
          try{
            let element = result[i]
            const exporterlistres = await ExporterModelV2.find({EXPORTER_NAME:element?.EXPORTER_NAME?.toString()})
            const ExporterMaster = exporterlistres?.[0]
            if(ExporterMaster){
              const crmTasks = await ExporterModelV2.find({EXPORTER_NAME:element?.EXPORTER_NAME?.toString()})
              if(crmTasks.length === 0){
                const TOP_COUNTRIES = await TTVModelV2.aggregate([
                  {
                    '$match': {
                      'EXPORTER_NAME': {
                        '$eq': element.EXPORTER_NAME.toString()
                      }
                    }
                  }, {
                    '$group': {
                      '_id': '$DESTINATION_COUNTRY', 
                      'FOB': {
                        '$sum': '$FOB_VALUE_USD'
                      }, 
                      'destination_country': {
                        '$first': '$DESTINATION_COUNTRY'
                      }, 
                      'total_shipments': {
                        '$sum': 1
                      }
                    }
                  }, {
                    '$sort': {
                      'FOB': -1
                    },
                  },
                  {
                    '$limit':2
                  }
                  
                ])
                let finalObj = {
                  EXPORTER_CODE:ExporterMaster.EXPORTER_CODE,
                  EXPORTER_NAME:ExporterMaster.EXPORTER_NAME,
                  EXPORTER_ADDRESS:element.EXPORTER_ADDRESS || "",
                  TOTAL_BUYERS:ExporterMaster.BUYERS.length,
                  EXPORTER_CITY: element.EXPORTER_CITY ? element.EXPORTER_CITY : ExporterMaster.EXPORTER_CITY,
                  FOB:ExporterMaster.FOB,
                  HS_CODE:HS_CODE,
                  HS_CODES:ExporterMaster.HS_CODES,
                  TOP_COUNTRIES: TOP_COUNTRIES,
                  EXTRA_DETAILS:element.EXTRA_DETAILS,
                  TOTAL_SHIPMENTS:ExporterMaster.TOTAL_SHIPMENTS
                }
                //await ExporterModel.updateOne({EXPORTER_CODE:element.EXPORTER_CODE?.toString()},{EXPORTER_CITY: element.EXPORTER_CITY })
                const res = await ExporterModelV2.create(finalObj)
              }else{
                let updatedArr = []
                // if(crmTasks[0].EXTRA_DETAILS){
                //   let extradetails = crmTasks[0].EXTRA_DETAILS
                //   let sheetExtradetails = element.EXTRA_DETAILS
                //   let combinedextradetails = [ ...extradetails,...sheetExtradetails ]

                //   const uniqueContacts = {}; // Object to keep track of unique combinations

                //   const updatedArr = combinedextradetails.filter(obj => {
                //     const contactPerson = obj['Contact Person'];
                //     const contactNumber = obj['Contact Number'];
                //     const key = contactPerson + '-' + contactNumber;
                //     if (!uniqueContacts[key]) {
                //       uniqueContacts[key] = true;
                //       return true; // Keep the first occurrence of the combination
                //     }
                //     return false; // Skip duplicates
                //   });
                  
                // }else{
                //   updatedArr = element.EXTRA_DETAILS
                // }
                //console.log('City',element.EXPORTER_CITY);
                updatedArr = element.EXTRA_DETAILS
                const response  = await ExporterModelV2.updateOne({EXPORTER_NAME:element.EXPORTER_NAME?.toString()},{EXPORTER_CITY: element.EXPORTER_CITY, EXTRA_DETAILS:updatedArr })
                //await ExporterModel.updateOne({EXPORTER_CODE:element.EXPORTER_CODE?.toString()},{EXPORTER_CITY: element.EXPORTER_CITY })
              }
            }else{
              not_added.push(element.EXPORTER_NAME)
            }
          }catch(e){
            console.log('error in addtask',e);
          }
        }
        fs.unlinkSync(filepath)
        resolve({
          success:true,
          message:'Tasks Added Succesfully',
          Failed:not_added
        })
      }

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}

exports.getAdminTasks = async (req,res) => {
  try{
    console.log(req.body ,"this islead req body-->>>")
    const result = await getAdminTasksFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getAdminTasksFunc = ({ currentPage ,resultPerPage, dateRangeFilter,taskUpdate,search,onlyShowForUserId,included_status,leadAssignedTo,hscodes,leadsStatus,requirements,taskStatus,TasksState,taskType,contactNo,subadminIds,TasksStateSearch   }) => {
  return new Promise(async(resolve, reject) => {
    try {
      console.log(taskUpdate,leadAssignedTo,leadsStatus,"here ar leads-->>>")
    let matchobj  = {}
    if(dateRangeFilter && dateRangeFilter.length >=1){
      if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
        matchobj = {
          $expr: {
            $eq: [
              { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
                dateRangeFilter?.[0]  // compare with the target date string
            ]
          }
        }
           
      }else{
        matchobj = {
          'TASK_DATE' :{
            $gte: new Date(dateRangeFilter?.[0]),
            $lte: new Date(dateRangeFilter?.[1])
           }
        }
      }
    }
    
    let includedTasks = []
    if(taskUpdate?.includes("User Onboarded")){
      if(taskUpdate && taskUpdate.length == 1){
        includedTasks = [4]
      }else{
        includedTasks.push(4)
      }
    }
    let mainPipeline = [
      {
        $match :{
          $and: [
            {TASK_TYPE: taskType},
            {STATUS : {$in:[0,1,2,3,4]}},
            { "TASK_ASSIGNED_TO.id" : {$exists : true}}
          ]
        }
      },
      { 
        $match : matchobj
      }
    ]
    if(onlyShowForUserId){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.id":onlyShowForUserId
        }
      })
    }
    if(subadminIds && subadminIds.length){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.id":{
            $in: subadminIds
          }
        }
      })
    }
    let FOB_BY_HS = null
    if(hscodes && hscodes.length){
      const hsCodesRegex = hscodes.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
      mainPipeline.push({
        $match: {
          "HS_CODES.HS_CODES": { $in: hsCodesRegex }
        }
      });
      FOB_BY_HS = {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$HS_CODES",
                as: "code",
                cond: {
                  $in: [
                    { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                    hscodes
                  ]
                }
              }
            },
            as: "code",
            in: "$$code.FOB_VALUE_USD"
          }
        }
      } 
    }
    if(requirements && requirements.length){
      mainPipeline.push({
        $match: {
          'INTRESTED_SERVICES' : {$in : requirements}
        }
      })
      
    }

    if(leadAssignedTo && leadAssignedTo.length){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
        }
      })
    }
    if (search) {
      let matchQuery;
  
      if (!isNaN(search)) { // Check if search is a number
          matchQuery = { 'EXTRA_DETAILS.Contact Number': { $regex: new RegExp(search), $options: 'i' } };
      } else if (/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/.test(search)) { // Check if search is an email address
          matchQuery = { 'EXTRA_DETAILS.Email ID': { $regex: new RegExp(search), $options: 'i' } };
      } else {
          matchQuery = {
              $or: [
                  { EXPORTER_NAME: { $regex: new RegExp(search), $options: 'i' } },
                  { EXPORTER_ADDRESS: { $regex: new RegExp(search), $options: 'i' } }
              ]
          };
      }
  
      mainPipeline.push({
          $match: matchQuery
      });
  }
 
   
    mainPipeline.push({
      $lookup: {
        from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
        localField: 'EXPORTER_CODE',
        foreignField: 'EXPORTER_CODE',
        as: 'task_logs'
      }
    })
    mainPipeline.push({
      $addFields :{
        "task_logs": {
          "$ifNull": ["$task_logs", []]
        }
      }
    })
    if(TasksStateSearch){
      mainPipeline.push({
        $match : {
          'task_logs.REMARK': {
            $regex : new RegExp(TasksStateSearch),
            $options:'i'
          }
        }
      })
    }

    let projectObj = {
      EXPORTER_ADDRESS:1,
      EXPORTER_CITY:1,
      EXPORTER_CODE:1,
      EXPORTER_NAME:1,
      EXTRA_DETAILS:1,
      FOB:1,
      STATUS:1,
      TASK_ASSIGNED_TO:1,
      TOP_COUNTRIES:1,
      TOTAL_BUYERS:1,
      LastNote: {$last: '$task_logs.REMARK'},
      LastEventTime: {$last: '$task_logs.CREATED_AT'},
      LastEventType : {$last: '$task_logs.EVENT_TYPE'},
      LAST_NOTE:1,
      LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
      HS_CODES:1,
      TOTAL_SHIPMENTS:1,
      EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
      TASK_DATE:1,
      task_logs:1,
      EXPORTER_COUNTRY:1,
      PRICING:1
    }
    
    if(FOB_BY_HS){
      projectObj["FOB"] = FOB_BY_HS
    }
    mainPipeline.push({
      $project : projectObj
    })
    if(TasksState && TasksState.length){
      if(TasksState.includes('Task Created') && TasksState.includes('Task Not Created')){
        // mainPipeline.push({
        //   $match: {
        //     'LastNote' : 
        //   }
        // })
      }else if(TasksState.includes('Task Created')){
        mainPipeline.push({
          $match: {
            'LOG_TYPE' : {
              $exists: true
            }
          }
        })
      }else if(TasksState.includes('Task Not Created')){
        mainPipeline.push({
          $match: {
            'LOG_TYPE' : {
              $exists: false
            }
          }
        })
      }
    }
    if(contactNo && contactNo.length){
      if(contactNo.includes('Number Available')){
        mainPipeline.push({
          $match: {
            'EXTRA_DETAILS.Contact Number' : {
              $exists: true
            }
          }
        })
      }else if(contactNo.includes('Number Not Available')){
        mainPipeline.push({
          $match: {
            'EXTRA_DETAILS.Contact Number' : {
              $exists: false
            }
          }
        })
      }
    }
    
    // mainPipeline.push({
    //   $sort : {
    //     'TASK_DATE': 1,
    //   } 
    // })
    if(taskStatus && taskStatus.length){
      mainPipeline.push({
        $match: {
          'EVENT_STATUS' : {
            $in : taskStatus.map(item => new RegExp(item))
          }
        }
      })
    } 
    // if(leadsStatus && leadsStatus.length){
    //   if(leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")){
    //     mainPipeline.push({
    //       $match:{
    //         'STATUS': {
    //           '$in': [0,1,2,3,4]
    //         }
    //       }
    //     })
    //   }else if(leadsStatus.includes("Lead Created")){
    //     mainPipeline.push({
    //       $match:{
    //         'STATUS': {
    //           '$in': [1]
    //         }
    //       }
    //     })
    //   }else if(leadsStatus.includes("Lead Not Created")){
    //     mainPipeline.push({
    //       $match:{
    //         'STATUS': {
    //           '$in': [0,2,3,4]
    //         }
    //       }
    //     })
    //   }
    // }


    if (leadsStatus && leadsStatus.length) {
      if (leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")) {
        mainPipeline.push({
          $match: {
            STATUS: { $in: [0, 1, 2, 3, 4] }
          }
        });
      } else if (leadsStatus.includes("Lead Created")) {
        mainPipeline.push({
          $match: {
            STATUS: { $in: [1] }
          }
        });
      } else if (leadsStatus.includes("Lead Not Created")) {
        mainPipeline.push({
          $match: {
            STATUS: { $in: [0, 2, 3, 4] }
          }
        });
      }
    }
    


    if(taskUpdate){
      let statusArray = taskUpdate.filter(element => element !== 'User Onboarded' && element !== 'Lead Created')
      if(statusArray && statusArray.length ){
        mainPipeline.push({
          $match:{
            $or : [
              {
                'STATUS': {
                  '$in': includedTasks
                }
              },
              {$and : [
                {'LOG_TYPE' : 'Didnt connect'},
                {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
              ]
              },
              {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
            ]
          }
        })
      }else{
        // mainPipeline.push({
        //   $match:{
        //     'STATUS': {
        //       '$in': includedTasks
        //     }
        //   }
        // })
      
        mainPipeline.push({
          $match:{
            $or : [
              {
                'STATUS': {
                  '$in': includedTasks
                }
              },
              statusArray && statusArray.length ? {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }} : {}
            ]
          }
        })
    }
    }else{
      if(!leadsStatus){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': included_status
            }
          }
        })
      }
    }
    
    const countpipeline = [...mainPipeline]
    let countoptimized = [...countpipeline]
    if(!(taskStatus || taskUpdate || TasksState || TasksStateSearch)){
      countoptimized = countpipeline.filter((stage) => !("$lookup" in stage))
    }
    countoptimized.push({
      '$count': 'total_records'
    })

    const countRes = await ExporterModelV2.aggregate(countoptimized)
    const total_count = countRes[0]?.total_records
    if(currentPage && resultPerPage) {
      mainPipeline.push({
        '$skip': (currentPage - 1) * parseInt(resultPerPage) 
      })
      mainPipeline.push({
        '$limit': parseInt(resultPerPage) 
      })
    }  
    
    // mainPipeline.push({
    //   $lookup:{
    //     from: 'tbl_exporters_lists',
    //     localField: 'EXPORTER_CODE',
    //     foreignField: 'EXPORTER_CODE',
    //     as: 'exporter_data'

    //   }})
      mainPipeline.push({
        $project : {
          EXPORTER_ADDRESS:1,
          EXPORTER_CITY:1,
          EXPORTER_CODE:1,
          EXPORTER_NAME:1,
          EXTRA_DETAILS:1,
          FOB:1,
          STATUS:1,
          TASK_ASSIGNED_TO:1,
          TOP_COUNTRIES:1,
          TOTAL_BUYERS:1,
          LastNote: 1,
          LastEventTime: 1,
          LastEventType : 1,
          LAST_NOTE:1,
          LOG_TYPE: 1,
          HS_CODES:1,
          TOTAL_SHIPMENTS:1,
          EVENT_STATUS:1,
          BUYERS: 1,
          export_data:1,
          DidntConnectCount: {
            "$size": {
              "$filter": {
                "input": "$task_logs",
                "cond": {
                  "$eq": ["$$this.LOG_TYPE", "Didnt connect"]
                }
              }
            }
          },
          EXPORTER_COUNTRY:1,
          PRICING:1
        }
      })
    if(taskType === 'Corporate'){
      mainPipeline.push({
        $lookup: {
          from: env === 'dev' ? 'tbl_crm_applications' : 'tbl_crm_applications_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'crm_applications'
        }
      })
    }
      const response = await ExporterModelV2.aggregate(mainPipeline)

      let query = `SELECT * FROM tbl_inquiry_from_website WHERE status=1`
      let dbRes = 
      resolve({
        success:true,
        message:{
          message:response,
          total_count
        }
      })

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}


exports.getExporterExtraDetails = async (req,res) => {
  try{
    const result = await getExporterExtraDetailsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getExporterExtraDetailsFunc = async ({EXPORTER_NAME,EXPORTER_COUNTRY}) => {
  return new Promise(async(resolve,reject) => {
    try{

      const Recent_Shipments = await TTV.find({EXPORTER_NAME:EXPORTER_NAME}).sort({DATE: -1}).limit(3);
      const TOP_Shipments = await TTV.find({EXPORTER_NAME:EXPORTER_NAME}).sort({FOB_VALUE_USD: -1}).limit(3);
      const TOP3_Buyers = await TTV.aggregate([
        {$match:  {EXPORTER_NAME:EXPORTER_NAME}},
        { $group: { _id: "$CONSIGNEE_NAME", count: { $sum: 1 } } },
        { $sort:  { count: -1 } },
        { $limit: 3 }
      ])
      const PRODUCTS = await TTV.aggregate([
        {$match : {EXPORTER_NAME:EXPORTER_NAME}},
        {$group : {_id : "$PRODUCT_TYPE"}}
      ])
      let returnObj = {
        EXPORTER_NAME,
        Recent_Shipments,
        TOP_Shipments,
        TOP3_Buyers,
        PRODUCTS
      }
      resolve({
        success:true,
        message:returnObj
      })
    }catch(e){
      console.log(e);
      reject({
        success:false,
        message:e
      })
    }
  })
}

exports.addNoteForCallList = async (req,res) => {
  try{
    let reqBody = req.body
    const updateQuery = await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE: reqBody.id},{LAST_NOTE:reqBody.adminNote})
    
    res.send({
      success:true,
      message:'Note Saved'
    })
  }catch(e){
    console.log('error in addNoteForEnquiry',e)
    res.send({
      success:false,
      message:'Failed to update Note.'
    })
  }
}

exports.getTopCountries = async (req,res) => {
  try{
    let { EXPORTER_NAME } = req.body
    const result = await TTV.aggregate([
      {
        '$match': {
          'EXPORTER_NAME': EXPORTER_NAME,
        }
      }, {
        '$group': {
          '_id': {
            'DESTINATION_COUNTRY': '$DESTINATION_COUNTRY', 
            'CONSIGNEE_NAME': '$CONSIGNEE_NAME'
          }, 
          'FOB': {
            '$sum': '$FOB_VALUE_USD'
          }, 
          'total_shipments': {
            '$sum': 1
          }
        }
      }, {
        '$sort': {
          'FOB': -1
        }
      }, {
        '$group': {
          '_id': '$_id.DESTINATION_COUNTRY', 
          'total_shipments': {
            '$sum': '$total_shipments'
          }, 
          'FOB': {
            '$sum': '$FOB'
          }, 
          'top_buyers': {
            '$push': {
              'buyer_name': '$_id.CONSIGNEE_NAME', 
              'shipment_count': '$total_shipments', 
              'FOB': '$FOB'
            }
          }
        }
      }, {
        '$project': {
          'destination_country': '$_id', 
          'total_shipments': 1, 
          'top_buyers': {
            '$slice': [
              '$top_buyers', 5
            ]
          }, 
          'FOB': 1
        }
      }, {
        '$sort': {
          'FOB': -1
        }
      }
    ])
    res.send({
      success:true,
      message:result
    })
  }catch(e){
    res.send({
      success:false,
      message:e
    })
  }
}

exports.updateCRMTask = async (req,res) => {
  try{
    const result =await updateCRMTaskFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const updateCRMTaskFunc = ({EXPORTER_CODE,EXPORTER_NAME,EVENT_TYPE,EVENT_STATUS,EVENT_TIME,REMINDER,REMARK,ASSIGN_TASK_TO,LOG_TYPE,LOST_REASON,INTRESTED_SERVICES,SELECTED_HS,CONTACT_PERSON,CONTACT_NUMBER,MEETING_LOCATION,MEETING_DURATION,MEETING_HEAD_COUNT,ADMIN_ID,ADMIN_NAME}) => {
  return new Promise(async(resolve,reject) => {
    try{
        if(LOG_TYPE === 'Create New Task'){
          await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE:EXPORTER_CODE},{REMINDER:REMINDER,TASK_DATE:EVENT_TIME,INTRESTED_SERVICES:INTRESTED_SERVICES,SELECTED_HS:SELECTED_HS})
          await CRMTasksLogs.create({
            EXPORTER_CODE,
            EXPORTER_NAME,
            EVENT_TYPE,
            EVENT_STATUS,
            EVENT_TIME,
            REMARK,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            MEETING_LOCATION,
            MEETING_DURATION,
            MEETING_HEAD_COUNT,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'Didnt connect'){
          await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE:EXPORTER_CODE},{TASK_DATE:EVENT_TIME})
          await CRMTasksLogs.create({
            EXPORTER_CODE,
            EXPORTER_NAME,
            EVENT_STATUS,
            REMARK,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            EVENT_TIME,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'Call back'){
          await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE:EXPORTER_CODE},{REMINDER:REMINDER,TASK_DATE:EVENT_TIME})
          await CRMTasksLogs.create({
            EXPORTER_CODE,
            EXPORTER_NAME,
            EVENT_STATUS,
            REMARK,
            EVENT_TIME,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'Not Interested'){
          await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE:EXPORTER_CODE},{STATUS:2,TASK_DATE:EVENT_TIME})
          await CRMTasksLogs.create({
            EXPORTER_CODE,
            EXPORTER_NAME,
            EVENT_STATUS,
            REMARK,
            LOG_TYPE,
            EVENT_TIME,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }
        else if(LOG_TYPE === 'Lead Lost'){
          await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE:EXPORTER_CODE},{STATUS:3})
          await CRMTasksLogs.create({
            EXPORTER_CODE,
            EXPORTER_NAME,
            EVENT_STATUS,
            REMARK,
            LOG_TYPE,
            LOST_REASON,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'Lead Created'){
          await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE:EXPORTER_CODE},{STATUS:1,REMINDER:REMINDER,TASK_DATE:EVENT_TIME})
          await CRMTasksLogs.create({
            EXPORTER_CODE,
            EXPORTER_NAME,
            EVENT_TYPE,
            EVENT_STATUS,
            EVENT_TIME,
            REMARK,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'User Onboarded'){
          await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE:EXPORTER_CODE},{STATUS:4})
          await CRMTasksLogs.create({
            EXPORTER_CODE,
            EXPORTER_NAME,
            LOG_TYPE,
            ADMIN_ID,
            ADMIN_NAME
          })
        }
        
        resolve({
          success:true,
          message:'Task Created Succesfully'
        })
      }catch(e){
        console.log('error in API', e);
        reject({
          success:false,
          message:'Task Creation Failed'
        })
    }
  })
}

exports.getCallListFilters = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filterData = {}
    const { dateRangeFilter,taskStatus,included_status,leadAssignedTo,onlyShowForUserId,type }  = req.body
    if(type != 'Corporate'){
      if(included_status && included_status.length === 1){
        filterData["Date"] = {
          "accordianId": 'dateRangeFilter',
          type: "minMaxDate",
          value: []
        }
      }else{
        const today = new Date();
        let startDate = new Date(today.getFullYear(), today.getMonth(), today.getDate());
        let endDate = new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1, 0, 0, -1);
        filterData["Date"] = {
          "accordianId": 'dateRangeFilter',
          type: "minMaxDate",
          value: [moment(startDate).format('YYYY-MM-DD'),moment(endDate).format('YYYY-MM-DD')],
          isFilterActive:true
        }
      }
    
    }
    

    filterData["Task"] = {
      "accordianId":"TasksState",
      type: "checkbox",
      labelName: "name",
      data:[{name:"Task Created"}, {name:"Task Not Created"}]
    }
    filterData["Task Update"] = {
      "accordianId": 'taskUpdate',
      type: "checkbox",
      labelName: "name"
    }
    filterData["Task Update"]["data"] = [{"name" : "Task"},{"name" : "Didnt connect"} ,{ name: "Busy"},
    { name: "Not Reachable"},
    { name: "Wrong Number" },
    { name: "Invalid Number" },
    { name: "Switched Off"},{"name" : "Call back"}  ,{"name" : "Not Interested"}, {"name":"Lead Lost"},{name: "User Onboarded"}]
    if(!reqBody.onlyShowForUserId){
      filterData["Lead Assigned To"] = {
        "accordianId": 'leadAssignedTo',
        type: "checkbox",
        labelName: "name"
      }
      let query = `SELECT tbl_user_details.contact_person AS name FROM tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user.id = tbl_user_details.tbl_user_id WHERE tbl_user.isSubUser = 1 AND tbl_user.type_id = 1 `
      let dbRes = await call({ query }, 'makeQuery', 'get');
      filterData["Lead Assigned To"]["data"] = dbRes.message
    }

    filterData["Leads"] = {
      "accordianId": 'leadsStatus',
      type: "checkbox",
      labelName: "name",
      data: [{name:"Lead Created"}, {name:"Lead Not Created"}]
    }

    filterData["Contact No"] = {
      "accordianId": 'contactNo',
      type: "checkbox",
      labelName: "name",
      data: [{name:"Number Available"}, {name:"Number Not Available"}]
    }

    filterData["Status"] = {
      "accordianId": 'taskStatus',
      type: "checkbox",
      labelName: "name",
      data:  [{ name: "Hot"}, {name: "Cold"}, {name: "Warm" }]
    }
    filterData["Requirement"] = {
      "accordianId": 'requirements',
      type: "checkbox",
      labelName: "name",
      data:  [
        { name: "Export LC discounting" },
        { name: "Export LC confirmation" },
        { name: "Import LC discounting" },
        { name: "Export invoice discounting" },
        { name: "SBLC" },
        { name: "Supply chain finance" },
        { name: "Import factoring" },
        { name: "Usance at sight" },
        { name: "Freight finance" },
        { name: "Packing credit" },
        { name: "Purchase order financing   " },
        { name: "Reverse factoring" },
        { name: "Trade credit insurance" }
      ]
    }
    const query = `SELECT code as name FROM tbl_hsn_codes`
    const dbRes = await call({query},'makeQuery','get')
    filterData["HS Code"] = {
      "accordianId": 'hscodes',
      type: "checkbox",
      labelName: "name",
      data:  dbRes.message
    }

    // let matchobj  = {}
    // if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
    //   matchobj = {
    //     $expr: {
    //       $eq: [
    //         { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
    //           dateRangeFilter?.[0]  // compare with the target date string
    //       ]
    //     }
    //   }
         
    // }else{
    //   matchobj = {
    //     'TASK_DATE' :{
    //       $gte: new Date(dateRangeFilter?.[0]),
    //       $lte: new Date(dateRangeFilter?.[1])
    //      }
    //   }
    // }
    // let mainPipeline = [
    //   {
    //     $match :{STATUS : {$in:included_status}}
    //   },
    //   { 
    //     $match : matchobj
    //   }
    // ]
    // if(onlyShowForUserId){
    //   mainPipeline.push({
    //     $match: {
    //       "TASK_ASSIGNED_TO.id":onlyShowForUserId
    //     }
    //   })
    // }
    // if(leadAssignedTo && leadAssignedTo.length){
    //   mainPipeline.push({
    //     $match: {
    //       "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
    //     }
    //   })
    // }
    // mainPipeline.push({
    //   $sort : {
    //     'FOB' : -1
    //   } 
    // })
    // mainPipeline.push({
    //   $lookup: {
    //     from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
    //     localField: 'EXPORTER_CODE',
    //     foreignField: 'EXPORTER_CODE',
    //     as: 'task_logs'
    //   }
    // })
    // mainPipeline.push({
    //   $project : {
    //     EXPORTER_NAME:1,
    //     EXTRA_DETAILS:1,
    //     LOG_TYPE: {$first: '$task_logs.LOG_TYPE'}
    //   }
    // })
    // if(taskStatus){
    //   mainPipeline.push({
    //     $match:{
    //       LOG_TYPE: {$in: taskStatus.map(item => new RegExp(item)) }
    //     }
    //   })
    // }
    // const extradetailsPipeline = [...mainPipeline,{
    //   $unwind:"$EXTRA_DETAILS"
    // },
    // {
    //   $group: {
    //     _id: null,
    //     'Contact_Person': {
    //       $addToSet : {
    //         name:'$EXTRA_DETAILS.Contact Person'
    //       }
    //     },
    //     'Contact_Number': {
    //       $addToSet : {
    //         name:'$EXTRA_DETAILS.Contact Number'
    //       }
    //     },
    //      'Designation': {
    //       $addToSet : {
    //         name:'$EXTRA_DETAILS.Designation'
    //       }
    //     }
    //   }
    // }
    // ]
    // const exporterNamePipeline = [...mainPipeline,{
    //   $group : {
    //     '_id': null,
    //     'EXPORTER_NAME':{
    //       '$addToSet': {
    //         'name' : '$EXPORTER_NAME'
    //       }
    //     }
    //   }
    // }]
    // const extradetailsResponse = await ExporterModelV2.aggregate(extradetailsPipeline)
    // const exporterNameResponse = await ExporterModelV2.aggregate(exporterNamePipeline)
    // filterData["Company Name"] = {
    //   "accordianId": 'companyName',
    //   type: "checkbox",
    //   labelName: "name",
    //   data : exporterNameResponse?.[0]?.EXPORTER_NAME
    // }


    // filterData["Contact No"] = {
    //   "accordianId": 'contactNo',
    //   type: "checkbox",
    //   labelName: "name",
    //   data: extradetailsResponse?.[0]?.Contact_Number
    // }

    // filterData["Contact Person"] = {
    //   "accordianId": 'contactPerson',
    //   type: "checkbox",
    //   labelName: "name",
    //   data: extradetailsResponse?.[0]?.Contact_Person
    // }

    // filterData["Designation"] = {
    //   "accordianId": 'designation',
    //   type: "checkbox",
    //   labelName: "name",
    //   data: extradetailsResponse?.[0]?.Designation
    // }
    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("error in getEnquiryAdminFilters", error);
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getLCINVGraphdata = async (req,res) => {
  try{
    let reqBody = req.body
    let dbRes 
    let tableDataForInvoiceLcApplication = []
    let response = {}

    let invSummary = { type: "Invoice Application", totalApplication: 0, approved: 0, approvedAmount: 0, rejected:0, rejectedAmount: 0,
    pending:0, pendingAmount: 0}
    let lcSummary = { type: "LC Application", totalApplication: 0, approved: 0, approvedAmount: 0, rejected:0, rejectedAmount: 0,
    pending:0, pendingAmount: 0}

    if (reqBody.invoiceLcApplicationFrom && reqBody.invoiceLcApplicationTo) {
      let dateRangeQueryForInvoice = ` AND created_at >= '${reqBody.invoiceLcApplicationFrom}' AND created_at <= '${reqBody.invoiceLcApplicationTo}'  `
      let dateRangeQueryForLC = ` AND updatedAt >= '${reqBody.invoiceLcApplicationFrom}' AND updatedAt <= '${reqBody.invoiceLcApplicationTo}'  `

      query = `SELECT id FROM tbl_invoice_discounting WHERE 1 ${dateRangeQueryForInvoice} AND seller_id=${req.body.userId}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      invSummary["totalApplication"] = dbRes.message.length

      query = `SELECT COUNT(id), SUM(contract_amount) FROM tbl_invoice_discounting WHERE (status = 3 OR status = 4 OR status = 6) ${dateRangeQueryForInvoice} AND seller_id=${req.body.userId}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      invSummary["approved"] = dbRes.message[0]["COUNT(id)"]
      invSummary["approvedAmount"] = dbRes.message[0]["SUM(contract_amount)"]

      query = `SELECT COUNT(id), SUM(contract_amount) FROM tbl_invoice_discounting WHERE status = 5 ${dateRangeQueryForInvoice} AND seller_id=${req.body.userId}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      invSummary["rejected"] = dbRes.message[0]["COUNT(id)"]
      invSummary["rejectedAmount"] = dbRes.message[0]["SUM(contract_amount)"]

      query = `SELECT COUNT(id), SUM(contract_amount) FROM tbl_invoice_discounting WHERE status NOT IN (3,4,5,6) ${dateRangeQueryForInvoice} AND seller_id=${req.body.userId}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      invSummary["pending"] = dbRes.message[0]["COUNT(id)"]
      invSummary["pendingAmount"] = dbRes.message[0]["SUM(contract_amount)"]

      // For LC
      // 0 pending
      // 1 Approved
      // 2 rejected
      // 3 Inprogress disbursement
      // 4 Disbursed

      query = `SELECT id FROM tbl_buyer_required_lc_limit WHERE invRefNo IS NOT NULL ${dateRangeQueryForLC} AND createdBy = ${req.body.userId}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      lcSummary["totalApplication"] = dbRes.message.length

      query = `SELECT COUNT(id), SUM(contractAmount) FROM tbl_buyer_required_lc_limit WHERE invRefNo IS NOT NULL AND (financeStatus = 1 OR financeStatus = 3 OR financeStatus = 4) ${dateRangeQueryForLC} AND createdBy = ${req.body.userId} `
      dbRes = await call({ query }, 'makeQuery', 'get');
      lcSummary["approved"] = dbRes.message[0]["COUNT(id)"]
      lcSummary["approvedAmount"] = dbRes.message[0]["SUM(contractAmount)"]

      query = `SELECT COUNT(id), SUM(contractAmount) FROM tbl_buyer_required_lc_limit WHERE invRefNo IS NOT NULL AND financeStatus = 2 ${dateRangeQueryForLC} AND createdBy = ${req.body.userId}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      lcSummary["rejected"] = dbRes.message[0]["COUNT(id)"]
      lcSummary["rejectedAmount"] = dbRes.message[0]["SUM(contractAmount)"]

      query = `SELECT COUNT(id), SUM(contractAmount) FROM tbl_buyer_required_lc_limit WHERE invRefNo IS NOT NULL AND financeStatus = 0 ${dateRangeQueryForLC} AND createdBy = ${req.body.userId}`
      dbRes = await call({ query }, 'makeQuery', 'get');
      lcSummary["pending"] = dbRes.message[0]["COUNT(id)"]
      lcSummary["pendingAmount"] = dbRes.message[0]["SUM(contractAmount)"]

      response["lcSummary"] = lcSummary
      response["invSummary"] = invSummary

      tableDataForInvoiceLcApplication.push(["LC Discounting",  lcSummary["approved"], (lcSummary["approvedAmount"] || 0), 
          lcSummary["rejected"], (lcSummary["rejectedAmount"] || 0), lcSummary["pending"], (lcSummary["pendingAmount"] || 0)])

      tableDataForInvoiceLcApplication.push(["Invoice Discounting", invSummary["approved"], (invSummary["approvedAmount"] || 0), 
          invSummary["rejected"], (invSummary["rejectedAmount"] || 0), invSummary["pending"], (invSummary["pendingAmount"] || 0) ])

      response["tableDataForInvoiceLcApplication"] = tableDataForInvoiceLcApplication
    }
    let dbResBuyerStats = await getBuyersStatsFunc(req.body)
    response["buyserStats"] = dbResBuyerStats.message

    res.send({
      success:true,
      message:response
    })
  }catch(e){
    console.log('error in getLCINVGraphdata',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.getBuyersStats = async (req,res) => {
  try{
    const result = await getBuyersStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getBuyersStatsFunc = ({userId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      const query = `SELECT 
      tbl_buyer_required_limit.selectedQuote, 
      tbl_buyers_detail.id,tbl_buyer_required_limit.id as limit_id,
      tbl_buyer_required_limit.buyers_credit,
      tbl_buyer_required_limit.requiredLimit
      FROM tbl_buyers_detail
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
      WHERE tbl_buyers_detail.user_id = ${userId}
      GROUP BY tbl_buyers_detail.id
      ORDER BY tbl_buyers_detail.modified_at DESC   ` 
      const dbRes = await call({query},'makeQuery','get')
      const response = dbRes.message
      let limit_applied_count= 0
      let limit_not_applied_count = 0
      let limit_available = 0
      let limit_available_count= 0
      for(let i=0;i<=response.length - 1;i++){
        const element = response[i]
        if(!element.limit_id){
          //limit not applied
          limit_not_applied_count += 1
        }else if(element.selectedQuote){
          //limit available
          limit_available_count += 1
          if(element.selectedQuote){
            const quote =  element.selectedQuote
            if(element.selectedQuote){
              if(quote.financeLimit){
                limit_available += parseFloat(quote.financeLimit)
              }
            }
            
          }
        } else if(!element.buyers_credit || (element.buyers_credit && !element.selectedQuote)){
          //limit applied 
          limit_applied_count +=1
        }
      }
      //disbursement done
      let disbursementQuery = `SELECT SUM(tbl_disbursement_scheduled.amount) as fundsDisbursed FROM tbl_disbursement_scheduled 
      LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.invRefNo = tbl_disbursement_scheduled.invRefNo
      WHERE tbl_buyer_required_limit.userId = ${userId} AND tbl_disbursement_scheduled.status = 1`
      const disbursementRes = await call({query:disbursementQuery},'makeQuery','get')
      let disbursement_done = disbursementRes.message?.[0]?.fundsDisbursed
      
      //finance applied
      const finAppliedQuery = `SELECT SUM(amount) as fin_applied FROM tbl_invoice_discounting WHERE status = 1 AND seller_id = ${userId}`
      const finAppliedRes = await call({query:finAppliedQuery},'makeQuery','get')
      let finance_applied = finAppliedRes.message?.[0]?.fin_applied
     
      //finance approved
      const finApprovedQuery = `SELECT SUM(amount) as fin_applied FROM tbl_invoice_discounting WHERE status = 3 AND seller_id = ${userId}`
      const finApprovedRes = await call({query:finApprovedQuery},'makeQuery','get')
      let finance_approved = finApprovedRes.message?.[0]?.fin_applied

      resolve({
        success:true,
        message: {
          limit_applied_count,
          limit_not_applied_count,
          limit_available,
          limit_available_count,
          disbursement_done,
          finance_applied,
          finance_approved
        }
      })

    }catch(e){
      console.log('error in getBuyersStatsFunc',e);
      resolve({
        success:false,
        message: {
          limit_applied_count:0,
          limit_not_applied_count:0,
          limit_available:0,
          limit_available_count:0,
          disbursement_done:0,
          finance_applied:0,
          finance_approved:0
        }
      })
    }
  })
}

exports.getApplicationStats = async (req,res) => {
  try{
    let response = {
      success:true,
      message: {}
    }
    const result = await getApplicationStatsFunc(req.body)
    response["message"]["applicationStats"] = result.message
    res.send(response)
  }catch(e){
    res.send(e)
  }
}

const getApplicationStatsFunc = ({userId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      const query = `SELECT 
      tbl_buyer_required_limit.selectedQuote, 
      tbl_buyers_detail.id,tbl_buyer_required_limit.id as limit_id,
      tbl_buyer_required_limit.buyers_credit,
      tbl_buyer_required_limit.requiredLimit
      FROM tbl_buyers_detail
      LEFT JOIN tbl_buyer_required_limit ON
      tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
      WHERE tbl_buyers_detail.user_id = ${userId}
      GROUP BY tbl_buyers_detail.id
      ORDER BY tbl_buyers_detail.id ASC  ` 
      const dbRes = await call({query},'makeQuery','get')
      const response = dbRes.message
      let limit_available = 0
      for(let i=0;i<=response.length - 1;i++){
        const element = response[i]
        if(element.selectedQuote){
          //limit available
          if(element.selectedQuote){
            const quote =  element.selectedQuote
            if(element.selectedQuote){
              if(quote.financeLimit){
                limit_available += parseFloat(quote.financeLimit)
              }
            } 
          }
        }
      }
      //disbursement done
      let disbursementQuery = `SELECT SUM(tbl_disbursement_scheduled.amount) as fundsDisbursed FROM tbl_disbursement_scheduled 
      LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.invRefNo = tbl_disbursement_scheduled.invRefNo
      WHERE tbl_buyer_required_limit.userId = ${userId} AND tbl_disbursement_scheduled.status = 1`
      const disbursementRes = await call({query:disbursementQuery},'makeQuery','get')
      let disbursement_done = disbursementRes.message?.[0]?.fundsDisbursed
      
      //finance applied
      const finAppliedQuery = `SELECT SUM(amount) as fin_applied FROM tbl_invoice_discounting WHERE status = 1 AND seller_id = ${userId}`
      const finAppliedRes = await call({query:finAppliedQuery},'makeQuery','get')
      let finance_applied = finAppliedRes.message?.[0]?.fin_applied
     
      //finance approved
      const finApprovedQuery = `SELECT SUM(amount) as fin_applied FROM tbl_invoice_discounting WHERE status = 3 AND seller_id = ${userId}`
      const finApprovedRes = await call({query:finApprovedQuery},'makeQuery','get')
      let finance_approved = finApprovedRes.message?.[0]?.fin_applied

      //finance rejected
      const finRejectedQuery = `SELECT SUM(amount) as fin_applied FROM tbl_invoice_discounting WHERE status = 5 AND seller_id = ${userId}`
      const finRejectedRes = await call({query:finRejectedQuery},'makeQuery','get')
      let finance_rejected= finRejectedRes.message?.[0]?.fin_applied

      //Ongoing Applications
      const applicationsQuery = ` SELECT *, (CASE 
        WHEN view_tasks.invRefNo = 'lc_discounting' THEN  'LC discounting (International)' 
        WHEN view_tasks.invRefNo = 'lc_confirmation' THEN 'LC confirmation (International)'
        WHEN view_tasks.invRefNo = 'lc_confirmation_discounting' THEN 'LC Confirmation & Discounting (International)'
        WHEN view_tasks.invRefNo = 'lc_discounting_domestic' THEN 'LC discounting (Domestic)'
        WHEN view_tasks.invRefNo = 'lc_confirmation_domestic' THEN 'LC confirmation (Domestic)'
        WHEN view_tasks.invRefNo = 'lc_confirmation_discounting_domestic' THEN 'LC Confirmation & Discounting (Domestic)'
        WHEN view_tasks.invRefNo = 'sblc' THEN 'SBLC'
        ELSE 'Invoice Discounting'
        END) as finance_type 
      FROM view_tasks WHERE userId = ${userId}`
      const applicationsRes = await call({query:applicationsQuery},'makeQuery','get')
      let lc_count = 0
      let invoice_count = 0
      for(let i=0;i<=applicationsRes.message.length - 1 ; i++){
        const element = applicationsRes.message[i]
        if(element.finance_type === 'Invoice Discounting'){
          invoice_count += 1
        }else {
          lc_count += 1
        }
      }

      resolve({
        success:true,
        message: {
          limit_available,
          disbursement_done,
          finance_applied,
          finance_approved,
          lc_count,
          invoice_count,
          total_application_count: lc_count + invoice_count,
          finance_rejected 
        }
      })

    }catch(e){
      console.log('error in getBuyersStatsFunc',e);
      resolve({
        success:false,
        message: {
          limit_available:0,
          disbursement_done:0,
          finance_applied:0,
          finance_approved:0,
          lc_count:0,
          invoice_count:0,
          total_application_count: 0,
          finance_rejected : 0
        }
      })
    }
  })
}

exports.getApplications = async(req,res) => {
  try{
    const result = await getApplicationsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getApplicationsFunc = ({userId,currentPage,resultPerPage,sortBydate,sortBuyerName,sortFinName,sortAmount,financiersFilter,applicationType,buyerName}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let extraQuery = ''
      if(buyerName){
        extraQuery = ` AND tbl_buyers_detail.buyerName IN (${buyerName.join(",")})`
      }
      let userSpecificQuery = ''
      if(userId){
        userSpecificQuery = ` AND view_tasks.userId = ${userId} `
      }
      let query =  `SELECT *
      FROM (
        SELECT view_tasks.*, 
        supplierDetails.company_name,
        (LENGTH(view_tasks.buyers_credit) - LENGTH(REPLACE(view_tasks.buyers_credit, '"financierAction":"deny"', '')))/LENGTH('"financierAction":"deny"') AS countOfDeniedQuotes,
        COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
        GROUP_CONCAT(DISTINCT tbl_user_details.company_name ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderName,
        GROUP_CONCAT(DISTINCT tbl_user_details.tbl_user_id ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderId,
               (CASE 
                WHEN view_tasks.invRefNo = 'lc_discounting' THEN  'LC discounting (International)' 
                WHEN view_tasks.invRefNo = 'lc_confirmation' THEN 'LC confirmation (International)'
                WHEN view_tasks.invRefNo = 'lc_confirmation_discounting' THEN 'LC Confirmation & Discounting (International)'
                WHEN view_tasks.invRefNo = 'lc_discounting_domestic' THEN 'LC discounting (Domestic)'
                WHEN view_tasks.invRefNo = 'lc_confirmation_domestic' THEN 'LC confirmation (Domestic)'
                WHEN view_tasks.invRefNo = 'lc_confirmation_discounting_domestic' THEN 'LC Confirmation & Discounting (Domestic)'
                WHEN view_tasks.invRefNo = 'sblc' THEN 'SBLC'
                ELSE 'Invoice Discounting'
                END) as finance_type,
        tbl_buyers_detail.buyerCountry,
        tbl_buyers_detail.termsOfPayment,
        GROUP_CONCAT(DISTINCT chat_rooms.chat_room_id ORDER BY chat_rooms.chat_room_id SEPARATOR ',') as chatRoomIds,
        GROUP_CONCAT(DISTINCT chat_rooms.included_users ORDER BY chat_rooms.chat_room_id SEPARATOR ',') as chatRoomUsers,
        IFNULL(um.unreadMsgCount, '0') AS chatRoomUnreadMsgCount,
        MAX(ar.remark) AS lastInternalRemark,
        GROUP_CONCAT(DISTINCT IFNULL(tbl_last_message.id, 'null') ORDER BY chat_rooms.chat_room_id) AS lastMessageIds
        FROM view_tasks 

        LEFT JOIN tbl_buyers_detail 
        ON tbl_buyers_detail.id = view_tasks.buyerId

        LEFT JOIN tbl_share_invoice_quote_request ON
        tbl_share_invoice_quote_request.quoteId = view_tasks.tblId

        LEFT JOIN tbl_user_details ON
        tbl_share_invoice_quote_request.lenderId = tbl_user_details.tbl_user_id

        LEFT JOIN tbl_user_details supplierDetails ON
        view_tasks.userId = supplierDetails.tbl_user_id

        LEFT JOIN tbl_admin_remarks ar ON ar.invApplicationId = view_tasks.tblId


        LEFT JOIN tbl_chat_rooms AS chat_rooms ON chat_rooms.invApplicationId = view_tasks.tblId
        LEFT JOIN tbl_last_message ON tbl_last_message.chat_room_id = chat_rooms.chat_room_id    

        LEFT JOIN (
          SELECT cr.invApplicationId, GROUP_CONCAT(IFNULL(um.count, '0') ORDER BY cr.chat_room_id SEPARATOR ',') AS unreadMsgCount
          FROM tbl_chat_rooms cr
          LEFT JOIN tbl_chatroom_unread_msg um ON cr.chat_room_id = um.chatRoomId AND um.userId = ${environment == "prod" ? "1" : "121"}
          GROUP BY cr.invApplicationId
        ) um ON um.invApplicationId = view_tasks.tblId 

        WHERE 1 AND (view_tasks.invRefNo IS NULL OR view_tasks.invRefNo  NOT LIKE '%lc%') ${userSpecificQuery} GROUP BY view_tasks.tblId ${extraQuery}
      ) subq WHERE 1`

      let lcquery =  `SELECT *
      FROM (
        SELECT view_tasks.*, 
        supplierDetails.company_name,
        (LENGTH(view_tasks.buyers_credit) - LENGTH(REPLACE(view_tasks.buyers_credit, '"status":"denied"', '')))/LENGTH('"status":"denied"') AS countOfDeniedQuotes,
        COUNT(tbl_user_details.company_name REGEXP ',') as countOfSelectedLender,
        GROUP_CONCAT(DISTINCT tbl_user_details.company_name ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderName,
        GROUP_CONCAT(DISTINCT tbl_user_details.tbl_user_id ORDER BY tbl_user_details.tbl_user_id) AS selectedLenderId,
               (CASE 
                WHEN view_tasks.invRefNo = 'lc_discounting' THEN  'LC discounting (International)' 
                WHEN view_tasks.invRefNo = 'lc_confirmation' THEN 'LC confirmation (International)'
                WHEN view_tasks.invRefNo = 'lc_confirmation_discounting' THEN 'LC Confirmation & Discounting (International)'
                WHEN view_tasks.invRefNo = 'lc_discounting_domestic' THEN 'LC discounting (Domestic)'
                WHEN view_tasks.invRefNo = 'lc_confirmation_domestic' THEN 'LC confirmation (Domestic)'
                WHEN view_tasks.invRefNo = 'lc_confirmation_discounting_domestic' THEN 'LC Confirmation & Discounting (Domestic)'
                WHEN view_tasks.invRefNo = 'sblc' THEN 'SBLC'
                ELSE 'Invoice Discounting'
                END) as finance_type,
        tbl_buyers_detail.buyerCountry,
        tbl_buyers_detail.termsOfPayment,
        GROUP_CONCAT(DISTINCT chat_rooms.chat_room_id ORDER BY chat_rooms.chat_room_id SEPARATOR ',') as chatRoomIds,
        GROUP_CONCAT(DISTINCT chat_rooms.included_users ORDER BY chat_rooms.chat_room_id SEPARATOR ',') as chatRoomUsers,
        IFNULL(um.unreadMsgCount, '0') AS chatRoomUnreadMsgCount,
        MAX(ar.remark) AS lastInternalRemark,
        tbl_buyer_required_lc_limit.reviewPending,
        tbl_buyer_required_lc_limit.quoteLocked,
        GROUP_CONCAT(DISTINCT IFNULL(tbl_last_message.id, 'null') ORDER BY chat_rooms.chat_room_id) AS lastMessageIds
        FROM view_tasks 
        
        LEFT JOIN tbl_buyers_detail 
        ON tbl_buyers_detail.id = view_tasks.buyerId

        LEFT JOIN tbl_buyer_required_lc_limit ON tbl_buyer_required_lc_limit.id = view_tasks.tblId

        LEFT JOIN tbl_share_lc_quote_request ON
        tbl_share_lc_quote_request.quoteId = view_tasks.tblId

        LEFT JOIN tbl_user_details ON
        tbl_share_lc_quote_request.lenderId = tbl_user_details.tbl_user_id

        LEFT JOIN tbl_user_details supplierDetails ON
        view_tasks.userId = supplierDetails.tbl_user_id

        LEFT JOIN tbl_admin_remarks ar ON ar.lcApplicationId = view_tasks.tblId


        LEFT JOIN tbl_chat_rooms AS chat_rooms ON chat_rooms.lcApplicationId = view_tasks.tblId
        LEFT JOIN tbl_last_message ON tbl_last_message.chat_room_id = chat_rooms.chat_room_id    

        LEFT JOIN (
          SELECT cr.lcApplicationId, GROUP_CONCAT(IFNULL(um.count, '0') ORDER BY cr.chat_room_id SEPARATOR ',') AS unreadMsgCount
          FROM tbl_chat_rooms cr
          LEFT JOIN tbl_chatroom_unread_msg um ON cr.chat_room_id = um.chatRoomId AND um.userId = ${environment == "prod" ? "1" : "121"}
          GROUP BY cr.lcApplicationId
        ) um ON um.lcApplicationId = view_tasks.tblId 

        WHERE 1 AND (view_tasks.invRefNo LIKE '%lc%') ${userSpecificQuery} GROUP BY view_tasks.tblId ${extraQuery}
      ) subq WHERE 1`

      let Countquery = `SELECT COUNT(*) as total_records
      FROM (
        SELECT view_tasks.*, 
               (CASE 
                  WHEN view_tasks.invRefNo = 'lc_discounting' THEN  'LC discounting (International)' 
                  WHEN view_tasks.invRefNo = 'lc_confirmation' THEN 'LC confirmation (International)'
                  WHEN view_tasks.invRefNo = 'lc_confirmation_discounting' THEN 'LC Confirmation & Discounting (International)'
                  WHEN view_tasks.invRefNo = 'lc_discounting_domestic' THEN 'LC discounting (Domestic)'
                  WHEN view_tasks.invRefNo = 'lc_confirmation_domestic' THEN 'LC confirmation (Domestic)'
                  WHEN view_tasks.invRefNo = 'lc_confirmation_discounting_domestic' THEN 'LC Confirmation & Discounting (Domestic)'
                  WHEN view_tasks.invRefNo = 'sblc' THEN 'SBLC'
                  ELSE 'Invoice Discounting'
                END) as finance_type,
                tbl_buyers_detail.buyerCountry 
        FROM view_tasks 
        LEFT JOIN tbl_buyers_detail 
        ON tbl_buyers_detail.id = view_tasks.buyerId
        WHERE 1 AND (view_tasks.invRefNo IS NULL OR view_tasks.invRefNo  NOT LIKE '%lc%') ${userSpecificQuery} ${extraQuery}
      ) subq
      
      WHERE 1`
    
    if(financiersFilter){
      let extraQuery = ` AND subq.FinancierName IN (${financiersFilter.join(",")})`
      lcquery += extraQuery
      query += extraQuery
      Countquery += extraQuery
    }  

   
    
    if(applicationType){
      const appType = applicationType.map(item => item.split(" ").join("_").toLowerCase())
      let extraQuery = ` AND subq.finance_type IN (${applicationType.join(",")})`
      query += extraQuery
      lcquery += extraQuery
      Countquery += extraQuery
    }

    if(sortBydate){
      query += ` ORDER BY subq.updated_at ${sortBydate}`
      lcquery += ` ORDER BY subq.updated_at ${sortBydate}`

    }  
    else if(sortBuyerName){
      query += ` ORDER BY tbl_buyers_detail.buyerName ${sortBuyerName}`
      lcquery += ` ORDER BY tbl_buyers_detail.buyerName ${sortBuyerName}`

    }
   else if(sortFinName){
      query += ` ORDER BY subq.FinancierName ${sortFinName}`
      lcquery += ` ORDER BY subq.FinancierName ${sortFinName}`

    }else if(sortAmount){
      query += ` ORDER BY subq.requiredLimit ${sortAmount}`
      lcquery += ` ORDER BY subq.requiredLimit ${sortAmount}`

    }
    else{
      query += ` ORDER BY subq.updated_at DESC`
      lcquery += ` ORDER BY subq.updated_at DESC`

    }
  
    const dbRes = await call({query},'makeQuery','get')
    const dbRes2 = await call({query:lcquery},'makeQuery','get')
    console.log('dbRes222222',lcquery)
    let dataToReturn = [...(dbRes?.message || []), ... (dbRes2?.message || [])]
    const dbCountRes = dataToReturn.length

    if(sortBydate){
      if(sortBydate === 'DESC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.updated_at > b.updated_at)
            return -1;
          if (a.updated_at < b.updated_at)
            return 1;
          return 0;
        })   
      }else if(sortBydate === 'ASC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.updated_at < b.updated_at)
            return -1;
          if (a.updated_at > b.updated_at)
            return 1;
          return 0;
        })    
      }
    }else if(sortBuyerName){
      if(sortBuyerName === 'DESC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.buyerName > b.buyerName)
            return -1;
          if (a.buyerName < b.buyerName)
            return 1;
          return 0;
        })   
      }else if(sortBuyerName === 'ASC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.buyerName < b.buyerName)
            return -1;
          if (a.buyerName > b.buyerName)
            return 1;
          return 0;
        })    
      }
    }else if(sortFinName){
      if(sortFinName === 'DESC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.FinancierName > b.FinancierName)
            return -1;
          if (a.FinancierName < b.FinancierName)
            return 1;
          return 0;
        })   
      }else if(reqBody.sortFinName === 'ASC'){
        dataToReturn = dataToReturn.sort((a, b) => {
          if (a.FinancierName < b.FinancierName)
            return -1;
          if (a.FinancierName > b.FinancierName)
            return 1;
          return 0;
        })    
      }
    }else if(sortAmount){
      if(sortAmount === 'DESC'){
        dataToReturn = dataToReturn.sort((a,b) => new Date(b.requiredLimit).getTime() - new Date(a.requiredLimit).getTime() )   
      }else if(sortAmount === 'ASC'){
        dataToReturn = dataToReturn.sort((a,b) => new Date(a.requiredLimit).getTime() - new Date(b.requiredLimit).getTime() )   
      }
    }
    else{
      dataToReturn = dataToReturn.sort((a,b) => new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime() )   
    }

    if(resultPerPage && currentPage){
      const startindex = (currentPage - 1) * resultPerPage;
      const endIndex = startindex + resultPerPage;
      dataToReturn  = dataToReturn.slice(startindex,endIndex)
    }

    resolve({
      success:true,
      message:{
        message : dataToReturn,
        total_records: dbCountRes
      }
    })
      
    }catch(e){
      console.log('error in getapplications',e);
      reject({
        success:false,
        message:'Failed to fetch data'
      })
    }
  })
}

exports.getApplicationsFilters = async (req,res) => {
  try{
    let userSpecificQuery = ''
    let viewQuery = ''
    if(req.body.userId){
      userSpecificQuery = ` AND user_id = '${req.body.userId}' `
      viewQuery =  ` AND userId = '${req.body.userId}' `
    }
    let filterData = {}
    filterData["Application Type"] = {
      "accordianId": 'applicationType',
      type: "checkbox",
      labelName: "name",
      data: [ {name:"Invoice Discounting"},...Object.entries(LCPurposeObject).map(([name, value]) => ({ name : value }))]
    }

    let query = `SELECT DISTINCT buyerName AS name FROM tbl_buyers_detail WHERE 1 ${userSpecificQuery} 
    ORDER BY  buyerName ASC `
    let dbRes = await call({query}, 'makeQuery', 'get');
    filterData["Buyer Name"] = {
      "accordianId": 'buyerName',
      type: "checkbox",
      data: dbRes.message,
      labelName: "name"
    }

    let query2 = `SELECT DISTINCT FinancierName AS name FROM view_tasks WHERE 1  ${viewQuery} AND FinancierName IS NOT NULL
    ORDER BY FinancierName ASC `
    let dbRes2 = await call({query:query2}, 'makeQuery', 'get');

    filterData["Financier Name"] = {
      "accordianId": 'financiersFilter',
      type: "checkbox",
      data: dbRes2.message,
      labelName: "name"
    }

    res.send({
      success:true,
      message:filterData
    })
  }catch(e){
    console.log('error in getApplicationsFilters',e);
    res.send({
      success:false,
      message:[]
    })
  }
}

exports.getHSCodes = async(req,res) => {
  try{
    let query = `SELECT * FROM tbl_hs_codes WHERE LENGTH(HS_CODE) = ${req.body.digits}`
    if(req.body.searchparam){
      query += ` AND HS_CODE LIKE '${req.body.searchparam}%'`
    }
    const dbRes = await call({query},'makeQuery','get')
    res.send({
      success:true,
      message:dbRes.message
    })
  }catch(e){
    console.log(e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.updateLeadAssignedToMaster = async (req,res) => {
  try{
    let reqBody = req.body
    //const updateQuery = await ExporterModelV2.findOneAndUpdate({EXPORTER_CODE: reqBody.id},{LAST_NOTE:reqBody.adminNote})
    
    //console.log("uupdatrees",updateQuery)
    res.send({
      success:true,
      message:'Note Saved'
    })
  }catch(e){
    console.log('error in addNoteForEnquiry',e)
    res.send({
      success:false,
      message:'Failed to update Note.'
    })
  }
}

exports.getCRMMasterdata = async(req,res) => {
  try{
    //const result = await getCRMMasterdataFunc (req.body)
    const {search, country_name, currentPage, resultPerPage, searchParam, HS_CODES, AVAILABLE_CONTACTS, TURNOVER_RANGE, CITIES, STATUS, ORGANIZATION_TYPE, companyName, contactPerson, contactNo, designation, sortBuyerCount, sortCity, sortCompanyName, sortContactPerson, sortTurnover, leadAssignedTo, sortleadAssigned, BUYERS, COUNTRIES,EXPORTER_CODES } = req.body   
    //res.send(result)
    const pipelinedata = [];
    let FOB_BY_HS = null


    if (companyName && companyName.length) {
      pipelinedata.push({
        $match: {
          'EXPORTER_NAME': { $in: companyName }
        }
      });
    }
  
    if (HS_CODES && HS_CODES.length) {
      const hsCodesRegex = HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
      pipelinedata.push({
        $match: {
          "HS_CODES.HS_CODES": { $in: hsCodesRegex }
        }
      });
      FOB_BY_HS = {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$HS_CODES",
                as: "code",
                cond: {
                  $in: [
                    { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                    HS_CODES
                  ]
                }
              }
            },
            as: "code",
            in: "$$code.FOB_VALUE_USD"
          }
        }
      } 
    }
    if(searchParam){
      if(!isNaN(parseInt(searchParam))){
        FOB_BY_HS = {
          $sum: {
            $map: {
              input: {
                $filter: {
                  input: "$HS_CODES",
                  as: "code",
                  cond: {
                    $regexMatch: {
                      input: "$$code.HS_CODES",
                      regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)
                    }
                  }
                }
              },
              as: "code",
              in: "$$code.FOB_VALUE_USD"
            }
          }
        }
      }
    }
    let  organiztionType = {}
    if(ORGANIZATION_TYPE && ORGANIZATION_TYPE.length >= 1){
      let newObj = []
      if(ORGANIZATION_TYPE.includes("Others") && ORGANIZATION_TYPE.length > 1){
        newObj.push({
          'EXPORTER_NAME':{
            $regex: new RegExp(ORGANIZATION_TYPE.filter(item => item !== 'Others').join("|")), $options:'i'
          }
        })
        newObj.push({
          'EXPORTER_NAME': { $not: {$regex:/PVT LTD|PUB LTD|LLP/,$options:'i'}}
        })
      }else if(ORGANIZATION_TYPE.includes("Others")){
        newObj.push({
          'EXPORTER_NAME': {
            $not: /pvt|pub|llp/i
          }
        })
      }else{
        newObj.push({
          'EXPORTER_NAME': {
            $regex:new RegExp(ORGANIZATION_TYPE.filter(item => item !== 'Others').join("|"),'i') , 
          }
        })
      }
      
      organiztionType = {
        $or : newObj
      }
    }
    const matchConditions = [
      searchParam ? {
        $or: [
          { 'EXPORTER_NAME': { $regex: new RegExp(`${searchParam}`, 'i') } },
          { "HS_CODES.HS_CODES": { $regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`) } }
        ]
      } : {},
      country_name ? { 'EXPORTER_COUNTRY': country_name } : {},
      EXPORTER_CODES ? {'EXPORTER_CODE': {$in: EXPORTER_CODES}} :{},
      search ? { 
        $or: [
          {EXPORTER_NAME: {$regex: new RegExp(search) , $options:'i'}},
          { 'EXTRA_DETAILS.Contact Number': {$regex: new RegExp(search),$options:'i'}}
        ] 
      } : {},
      BUYERS && BUYERS.length ? { 'BUYERS': { $in: BUYERS } } : {},
      COUNTRIES && COUNTRIES.length ? { 'EXPORT_COUNTRIES': { $in: COUNTRIES } } : {},
      ORGANIZATION_TYPE && ORGANIZATION_TYPE.length ? organiztionType : {},
      CITIES && CITIES.length ? { 'EXPORTER_CITY': { $in: CITIES } } : {}
    ];
  
    const matchStage = {
      $match: {
        $and :matchConditions
      }
    };
  
    const lookupStage = {
      $lookup:{
        from: env === 'dev' ? 'tbl_crm_tasks' : 'tbl_crm_tasks_prod',
        localField: 'EXPORTER_CODE',
        foreignField: 'EXPORTER_CODE',
        as: 'crm_tasks'
      }
    };
    let projectStage = {
      $project : {
        EXPORTER_NAME: 1,
        EXPORTER_ADDRESS: 1,
        FOB: 1,
        EXPORTER_CODE: 1,
        EXPORTER_CITY: 1,
        EXTRA_DETAILS: {$first: '$crm_tasks.EXTRA_DETAILS'},
        TASK_ASSIGNED_TO:{$first:"$crm_tasks.TASK_ASSIGNED_TO"},
        TOTAL_BUYERS: {
          $size: {
             $ifNull: ['$BUYERS', []]
           }
        },
        BUYERS:1,
        STATUS:{$first : '$crm_tasks.STATUS'},
        HS_CODES:1,
        CIN_NO:1,
        AUDITOR_DATA:1,
        'ADMIN_ID':{
          $first : {
            $first : '$crm_tasks.TASK_ASSIGNED_TO.id'
          }
        }
      }
    }
    if(FOB_BY_HS){
      projectStage["$project"]["FOB_BY_HS"] = FOB_BY_HS
    }
    pipelinedata.push(matchStage);
    let contactFilter = {}
    if(AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length){
      let newObj= []
      for(let i=0;i<=AVAILABLE_CONTACTS.length - 1 ; i++){
        const element = AVAILABLE_CONTACTS[i]
        if(element.alt === 'contact_count'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
              {"EXTRA_DETAILS.Email ID" : {$exists:false}}
            ]
          })
        }else if(element.alt === 'email_count'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
              {"EXTRA_DETAILS.Email ID" : {$exists:true}}
            ]
          })
        }else if(element.alt === 'both_count'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
              {"EXTRA_DETAILS.Email ID" : {$exists:true}}
            ]
          })
        }else if(element.alt === 'both_not'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
              {"EXTRA_DETAILS.Email ID" : {$exists:false}}
            ]
          })
        }
      }
      contactFilter = {
        $or : newObj
      }
    }
    let  turnoverFilter = {}
    if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
      let newObj = []
      for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
        const element = TURNOVER_RANGE[i]
        if(element.minVal !== undefined && element.maxVal !== undefined){
          newObj.push({
            [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB'] : {
              $gte:element.minVal,
              $lte:element.maxVal
            }
          })
        }else{
          newObj.push({
            [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB']:{
              $gte:element.maxVal
            }
          })
        }
      }
      turnoverFilter = {
        $or : newObj
      }
    }
    let statusFilter = {}
    if(STATUS && STATUS.length){
      let newObj=[]
      if(areArraysOfObjectsIdentical(statusArr,STATUS,"name")){
        newObj.push(
          {"$or": [ {
            "ADMIN_ID": {
              "$ne": null
            }
          }]}
        )
      }else if(isArraySubsetOfAnother(statusArr,STATUS,"name")){
        newObj.push(
          {"$or": [ {
            "ADMIN_ID": {
              "$ne": null
            }
          }]}
        )
        for(let i = 0; i<= STATUS.length - 1;i++){
          const element = STATUS[i]
          if(!isStringInArrayOfObjects(statusArr,element.name)){
            if(element.status != undefined || element.status != null){
           
              if(element.status === 0){
               newObj.push({
                 'STATUS' : {"$ne": null}
               })
              }else if(element.status === 'Pending'){
               newObj.push({
                 $and: [
                   {'STATUS' : 0},
                   {
                     "$or": [ {
                       "ADMIN_ID": {
                         "$ne": null
                       }
                     }]
                   }
                 ]
               })
              }
              else{
               newObj.push({
                 'STATUS': element.status
                })
              }
             }else if(element.name === 'Not assigned'){
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$eq": null
                   }
                 }]} 
               )
             }else{
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$ne": null
                   }
                 }]}
               )
             }
          }
      
        }
      }else{
        for(let i = 0; i<= STATUS.length - 1;i++){
          const element = STATUS[i]
          if(element.status != undefined || element.status != null){
           
              if(element.status === 0){
               newObj.push({
                $and: [
                  {'STATUS' : {"$ne": null}},
                  {
                    "$or": [ {
                      "ADMIN_ID": {
                        "$ne": null
                      }
                    }]
                  }
                ]
                 
               })
              }else if(element.status === 'Pending'){
               newObj.push({
                 $and: [
                   {'STATUS' : 0},
                   {
                     "$or": [ {
                       "ADMIN_ID": {
                         "$ne": null
                       }
                     }]
                   }
                 ]
               })
              }
              else{
               newObj.push({
                $and: [
                  {'STATUS': element.status},
                  {
                    "$or": [ {
                      "ADMIN_ID": {
                        "$ne": null
                      }
                    }]
                  }
                ]
                 
                })
              }
             }else if(element.name === 'Not assigned'){
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$eq": null
                   }
                 }]} 
               )
             }else{
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$ne": null
                   }
                 }]}
               )
             }
          
      
        }
      }
      
      
      statusFilter = {
        $or : newObj
      }
      
    }
    let extraMatchObj = [
      leadAssignedTo && leadAssignedTo.length ? { 'TASK_ASSIGNED_TO.contact_person': { $in: leadAssignedTo } } : {},
      contactNo && contactNo.length ? {
        $or: [
          { 'EXTRA_DETAILS.Contact Number': { $in: contactNo } },
          { 'EXTRA_DETAILS.Contact Number': { $in: contactNo.map(item => item.toString()) } }
        ]
      } : {},
      contactPerson && contactPerson.length ? { 'EXTRA_DETAILS.Contact Person': { $in: contactPerson } } : {},
      designation && designation.length ? { 'EXTRA_DETAILS.Designation': { $in: designation } } : {},
      TURNOVER_RANGE && TURNOVER_RANGE.length ? turnoverFilter : {},
      STATUS && STATUS.length ? statusFilter : {},
      AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length? contactFilter : {}
    ]
    const extramatchStage = {
      $match: {
        $and : extraMatchObj
      }
    };
    const totalCountPipeline = [...pipelinedata,lookupStage,projectStage,extramatchStage,{$count: "dbCount" }];
    const dataPipeline = [...pipelinedata,lookupStage,projectStage,extramatchStage];
    
    // dataPipeline.push({
    //   $sort:{
    //     [FOB_BY_HS? 'FOB_BY_HS' :'FOB']:-1
    //   }
    // })
    if(sortBuyerCount){
      dataPipeline.push({
        $sort:{
          'TOTAL_BUYERS': sortBuyerCount
        }
      })
    }else if(sortCity){
      dataPipeline.push({
        $sort:{
          'EXPORTER_CITY': sortCity
        }
      })
    }else if(sortCompanyName){
      dataPipeline.push({
        $sort:{
          'EXPORTER_NAME': sortCompanyName
        }
      })
    }else if(sortContactPerson){
      dataPipeline.push({
        $sort:{
          'EXTRA_DETAILS.Contact Person': sortContactPerson
        }
      })
    }else if(sortTurnover){
      dataPipeline.push({
        $sort:{
          [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB']: sortTurnover
        }
      })
    }else if(sortleadAssigned){
      dataPipeline.push({
        $sort:{
          'TASK_ASSIGNED_TO.contact_person': sortleadAssigned
        }
      })
    }
    else if(FOB_BY_HS){
      dataPipeline.push({
        $sort:{
          'FOB_BY_HS': -1
        }
      })
    }else{
      dataPipeline.push({
        $sort:{
          'FOB': -1
        }
      })
    }
    const paginationStages = [
      { $skip: (currentPage - 1) * resultPerPage },
      { $limit: parseInt(resultPerPage)  }
    ];
  
    if (currentPage && resultPerPage) {
      dataPipeline.push(...paginationStages);
    }
    const countPromise = ExporterModel.aggregate(totalCountPipeline).exec();
    const dataPromise = ExporterModel.aggregate(dataPipeline).exec();
    const [countResult, dataResult] = await Promise.all([countPromise, dataPromise])
    const totalCount = countResult[0] ? countResult[0].dbCount : 0;
        
    res.send({ 
      success:true,
      message:{
        message: dataResult,
        total_records: totalCount
      }
    }); 
    // .then(([countResult, dataResult]) => {
    //     const totalCount = countResult[0] ? countResult[0].dbCount : 0;
        
    //     res.send({ 
    //       success:true,
    //       message:{
    //         message: dataResult,
    //         total_records: totalCount
    //       }
    //     });
    //   })
    //   .catch(error => {
    //     console.log('Error in crm master',error);
    //     throw new Error(`Error retrieving CRM master data: ${error.message}`);
    //   });
  }catch(e){
    console.log('Error in data',e);
    res.send(e)
  }
}

const getCRMMasterdataFunc = ({ search, country_name, currentPage, resultPerPage, searchParam, HS_CODES, AVAILABLE_CONTACTS, TURNOVER_RANGE, CITIES, STATUS, ORGANIZATION_TYPE, companyName, contactPerson, contactNo, designation, sortBuyerCount, sortCity, sortCompanyName, sortContactPerson, sortTurnover, leadAssignedTo, sortleadAssigned, BUYERS, COUNTRIES }) => {
  const pipelinedata = [];

  if (companyName && companyName.length) {
    pipelinedata.push({
      $match: {
        'EXPORTER_NAME': { $in: companyName }
      }
    });
  }

  if (HS_CODES && HS_CODES.length) {
    const hsCodesRegex = HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
    pipelinedata.push({
      $match: {
        "HS_CODES.HS_CODES": { $in: hsCodesRegex }
      }
    });
  }
  
  const matchConditions = [
    searchParam ? {
      $or: [
        { 'EXPORTER_NAME': { $regex: new RegExp(`${searchParam}`, 'i') } },
        { "HS_CODES.HS_CODES": { $regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`) } }
      ]
    } : {},
    country_name ? { 'EXPORTER_COUNTRY': country_name } : {},
    search ? { 'EXPORTER_NAME': { $regex: new RegExp(search, 'i') } } : {},
    BUYERS && BUYERS.length ? { 'BUYERS': { $in: BUYERS } } : {},
    COUNTRIES && COUNTRIES.length ? { 'EXPORT_COUNTRIES': { $in: COUNTRIES } } : {},
    ORGANIZATION_TYPE && ORGANIZATION_TYPE.length ? { 'EXPORTER_NAME': { $regex: ORGANIZATION_TYPE.join("|"), $options: "i" } } : {},
    CITIES && CITIES.length ? { 'EXPORTER_CITY': { $in: CITIES } } : {}
  ];

  const matchStage = {
    $match: {
      $and : matchConditions
    }
  };

  const lookupStage = {
    $lookup:{
      from: env === 'dev' ? 'tbl_crm_tasks' : 'tbl_crm_tasks_prod',
      localField: 'EXPORTER_CODE',
      foreignField: 'EXPORTER_CODE',
      as: 'crm_tasks'
    }
  };
  const projectStage = {
    $project : {
      EXPORTER_NAME: 1,
      EXPORTER_ADDRESS: 1,
      FOB: 1,
      EXPORTER_CODE: 1,
      EXPORTER_CITY: 1,
      EXTRA_DETAILS: {$first: '$crm_tasks.EXTRA_DETAILS'},
      TASK_ASSIGNED_TO:{$first:"$crm_tasks.TASK_ASSIGNED_TO"},
      TOTAL_BUYERS: {
        $size: {
           $ifNull: ['$BUYERS', []]
         }
      },
      BUYERS:1,
      STATUS:{$first : '$crm_tasks.STATUS'},
      HS_CODES:1
    }
  }

  pipelinedata.push(matchStage);
  let extraMatchObj = [
    leadAssignedTo && leadAssignedTo.length ? { 'TASK_ASSIGNED_TO.contact_person': { $in: leadAssignedTo } } : {},
    contactNo && contactNo.length ? {
      $or: [
        { 'EXTRA_DETAILS.Contact Number': { $in: contactNo } },
        { 'EXTRA_DETAILS.Contact Number': { $in: contactNo.map(item => item.toString()) } }
      ]
    } : {},
    contactPerson && contactPerson.length ? { 'EXTRA_DETAILS.Contact Person': { $in: contactPerson } } : {},
    designation && designation.length ? { 'EXTRA_DETAILS.Designation': { $in: designation } } : {},
    TURNOVER_RANGE && TURNOVER_RANGE.length ? { 'TURNOVER_RANGE': { $in: TURNOVER_RANGE } } : {},
    STATUS && STATUS.length ? { 'status': { $in: STATUS } } : {},
    AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length? { 'AVAILABLE_CONTACTS': { $exists: AVAILABLE_CONTACTS } } : {}
  ]
  const extramatchStage = {
    $match: {
      $and: extraMatchObj
    }
  };
  const totalCountPipeline = [...pipelinedata,lookupStage,projectStage,extramatchStage,{$count: "dbCount" }];
  const dataPipeline = [...pipelinedata,lookupStage,projectStage,extramatchStage];
  dataPipeline.push({
    $sort:{
      'FOB':-1
    }
  })
  const paginationStages = [
    { $skip: (currentPage - 1) * resultPerPage },
    { $limit: parseInt(resultPerPage)  }
  ];

  if (currentPage && resultPerPage) {
    dataPipeline.push(...paginationStages);
  }
  const countPromise = ExporterModel.aggregate(totalCountPipeline).exec();
  const dataPromise = ExporterModel.aggregate(dataPipeline).exec();

  return Promise.all([countPromise, dataPromise])
    .then(([countResult, dataResult]) => {
      const totalCount = countResult[0] ? countResult[0].dbCount : 0;
      return { 
        success:true,
        message:{
          message: dataResult,
          total_records: totalCount
        }};
    })
    .catch(error => {
      console.log('Error in crm master',error);
      throw new Error(`Error retrieving CRM master data: ${error.message}`);
    });
};


exports.AssignMasterDataTask = async(req,res) => {
  try{
    const result =  await  AssignMasterDataTaskFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const AssignMasterDataTaskFunc = ({EXPORTER_CODE, LeadAssignedObj}) => {
  return new Promise(async(resolve,reject) =>{
    try{
      const crmTasks = await ExporterModelV2.find({EXPORTER_CODE:EXPORTER_CODE})
      if(crmTasks.length >= 1){
        //Data Already Exists just update the assignee
        try{
          const update = await ExporterModelV2.updateOne({EXPORTER_CODE:EXPORTER_CODE},{TASK_ASSIGNED_TO:LeadAssignedObj})
          return resolve({
            success:true,
            message:'Lead Updated Succesfully'
          })
        }catch(e) {
          return reject({
            success:true,
            message:'Failed to update lead'
          })
        }
        
      }
      const exporterlist = await ExporterModel.find({EXPORTER_CODE:EXPORTER_CODE})
      if(exporterlist.length >= 1){
        const tommDate =  new Date()
        tommDate.setDate(tommDate.getDate() + 1)
        const exporterObj = exporterlist[0]
        const crmObj = {
          EXPORTER_CODE:exporterObj.EXPORTER_CODE,
          EXPORTER_NAME:exporterObj.EXPORTER_NAME,
          EXPORTER_ADDRESS:exporterObj.EXPORTER_ADDRESS,
          EXPORTER_CITY: exporterObj.EXPORTER_CITY,
          TOTAL_BUYERS:exporterObj.BUYERS.length,
          TOTAL_SHIPMENTS:exporterObj.TOTAL_SHIPMENTS,
          FOB: exporterObj.FOB,
          HS_CODES:exporterObj.HS_CODES,
          EXTRA_DETAILS:[],
          TASK_ASSIGNED_TO:LeadAssignedObj,
          STATUS:0        
        }

        const TOP_COUNTRIES = await TTV.aggregate([
          {
            '$match': {
              'EXPORTER_CODE': new RegExp(`^${exporterObj.EXPORTER_CODE}`)
            }
          }, {
            '$group': {
              '_id': '$DESTINATION_COUNTRY', 
              'total_shipments': {
                '$sum': 1
              }, 
              'FOB': {
                '$sum': '$FOB_VALUE_USD'
              }, 
              'destination_country': {
                '$first': '$DESTINATION_COUNTRY'
              }
            }
          }, {
            '$sort': {
              'FOB': -1
            }
          }, {
            '$limit': 2
          }
        ])
        crmObj["TOP_COUNTRIES"] = TOP_COUNTRIES

        const insertTask = await ExporterModelV2.create(crmObj)
        return resolve({
          success:true,
          message:'Lead Assigned Succesfully'
        })
      }
    }catch(e){
      console.log('error in addmASTERtASK',e);
      reject({
        success:false,
        message:'Failed to assign lead'
      })
    }
  })
}

exports.getCRMMasterFilterCount = async(req,res) => {
  try{
    const result = await getCRMMasterFilterCountFunc (req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCRMMasterFilterCountFunc = ({search,country_name,searchParam,HS_CODES,AVAILABLE_CONTACTS,TURNOVER_RANGE,CITIES,STATUS,ORGANIZATION_TYPE,companyName,contactPerson,contactNo,designation,sortBuyerCount,sortCity,sortCompanyName,sortContactPerson,sortTurnover,leadAssignedTo,sortleadAssigned}) => { 
  return new Promise(async(resolve,reject) => {
    try{
      const pipelinedata = []
      let searchObj = {}
      let FOB_BY_HS = null
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        if(!isNaN(parseInt(searchParam))){
          FOB_BY_HS = {
            $sum: {
              $map: {
                input: {
                  $filter: {
                    input: "$HS_CODES",
                    as: "code",
                    cond: {
                      $regexMatch: {
                        input: "$$code.HS_CODES",
                        regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)
                      }
                    }
                  }
                },
                as: "code",
                in: "$$code.FOB_VALUE_USD"
              }
            }
          }
        }
      }
      let hsObj ={}
      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
        }
        FOB_BY_HS = {
          $sum: {
            $map: {
              input: {
                $filter: {
                  input: "$HS_CODES",
                  as: "code",
                  cond: {
                    $in: [
                      { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                      HS_CODES
                    ]
                  }
                }
              },
              as: "code",
              in: "$$code.FOB_VALUE_USD"
            }
          }
        }
      }
      if(companyName && companyName.length){
        pipelinedata.push({
          $match:{
            'EXPORTER_NAME' : {$in:companyName}
          }
        })
      }
      pipelinedata.push({
        $match : {
          $and : [
            searchObj,
            {
              'EXPORTER_COUNTRY': {
                $regex: new RegExp(country_name),
                $options:'i'
              }
            },
            hsObj
          ]
        }
      })
      if(search){
        pipelinedata.push({
          $match: {
            'EXPORTER_NAME': {$regex : new RegExp(search),$options:'i'}
          }
        })
      }
      if(ORGANIZATION_TYPE && ORGANIZATION_TYPE.length){
        pipelinedata.push({
          $match :{
            EXPORTER_NAME: {
              $regex : ORGANIZATION_TYPE.join("|"),
              $options: "i"
            }
          }
        })
      }
     
      if(CITIES && CITIES.length){
        pipelinedata.push({
          $match:{
            'EXPORTER_CITY' : {$in : CITIES }
          }
        })
      }

      pipelinedata.push({
        $lookup:{
          from: env === 'dev' ? 'tbl_crm_tasks' : 'tbl_crm_tasks_prod',
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'crm_tasks'
        }
      })
      let projectObj = {
        EXPORTER_NAME: 1,
        EXPORTER_ADDRESS: 1,
        FOB: 1,
        EXPORTER_CODE: 1,
        EXPORTER_CITY: 1,
        EXTRA_DETAILS: {$first: '$crm_tasks.EXTRA_DETAILS'},
        TASK_ASSIGNED_TO:{$first:"$crm_tasks.TASK_ASSIGNED_TO"},
        TOTAL_BUYERS: {
          $size: {
             $ifNull: ['$BUYERS', []]
           }
        },
        STATUS:{$first : '$crm_tasks.STATUS'},
        "ADMIN_ID": {
          "$first": {
            "$first": "$crm_tasks.TASK_ASSIGNED_TO.id"
          }
        },
      }
      if(FOB_BY_HS){
        projectObj["FOB_BY_HS"] = FOB_BY_HS
      }
      pipelinedata.push({
        $project: projectObj
      })
      if(leadAssignedTo && leadAssignedTo.length){
        pipelinedata.push({
          $match : {
            'TASK_ASSIGNED_TO.contact_person' : {$in : leadAssignedTo}
          }
        })
      }
      if(contactNo && contactNo.length){
        pipelinedata.push({
          $match : {
            '$or': [{
              'EXTRA_DETAILS.Contact Number': {$in : contactNo}
            }, {
              'EXTRA_DETAILS.Contact Number': {
                '$in':contactNo.map(item => item.toString())
              }
            }]
          }
        })
      }
      if(contactPerson && contactPerson.length){
        pipelinedata.push({
          $match : {
            'EXTRA_DETAILS.Contact Person' : {$in : contactPerson}
          }
        })
      }
      if(designation && designation.length){
        pipelinedata.push({
          $match : {
            'EXTRA_DETAILS.Designation' : {$in : designation}
          }
        })
      }
      if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
        let newObj = []
        for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
          const element = TURNOVER_RANGE[i]
          if(element.minVal && element.maxVal){
            newObj.push({
              [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB'] : {
                $gte:element.minVal,
                $lte:element.maxVal
              }
            })
          }else{
            newObj.push({
              [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB']:{
                $gte:element.maxVal
              }
            })
          }
        }
        pipelinedata.push({
          $match:{
            $or : newObj
          }
        })
      }
      if(sortBuyerCount){
        pipelinedata.push({
          $sort:{
            'TOTAL_BUYERS': sortBuyerCount
          }
        })
      }else if(sortCity){
        pipelinedata.push({
          $sort:{
            'EXPORTER_CITY': sortCity
          }
        })
      }else if(sortCompanyName){
        pipelinedata.push({
          $sort:{
            'EXPORTER_NAME': sortCompanyName
          }
        })
      }else if(sortContactPerson){
        pipelinedata.push({
          $sort:{
            'EXTRA_DETAILS.Contact Person': sortContactPerson
          }
        })
      }else if(sortTurnover){
        pipelinedata.push({
          $sort:{
            [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB']: sortTurnover
          }
        })
      }else if(sortleadAssigned){
        pipelinedata.push({
          $sort:{
            'TASK_ASSIGNED_TO.contact_person': sortleadAssigned
          }
        })
      }
      else if(FOB_BY_HS){
        pipelinedata.push({
          $sort:{
            'FOB_BY_HS': -1
          }
        })
      }else{
        pipelinedata.push({
          $sort:{
            'FOB': -1
          }
        })
      }

      if(STATUS && STATUS.length){
        let newObj=[]
        if(areArraysOfObjectsIdentical(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
        }else if(isArraySubsetOfAnother(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(!isStringInArrayOfObjects(statusArr,element.name)){
              if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                   'STATUS' : {"$ne": null}
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                   'STATUS': element.status
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            }
        
          }
        }else{
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                  $and: [
                    {'STATUS' : {"$ne": null}},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                  $and: [
                    {'STATUS': element.status},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            
        
          }
        }
        
        
        statusFilter = {
          $or : newObj
        }
        
      }
      if(AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length){
        let newObj= []
        for(let i=0;i<=AVAILABLE_CONTACTS.length - 1 ; i++){
          const element = AVAILABLE_CONTACTS[i]
          if(element.alt === 'contact_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:false}}
              ]
            })
          }else if(element.alt === 'email_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_not'){
            newObj.push( {
              "EXTRA_DETAILS" : {$exists:false}
            })
          }
        }
        pipelinedata.push({
          $match:{
            $or : newObj
          }
        })
      }
      const Unassigned = [ ...pipelinedata]
      Unassigned.push({
        $match: {
          'TASK_ASSIGNED_TO.id':{$exists:false}
        }
      })
      pipelinedata.push({
        $match: {
          'TASK_ASSIGNED_TO.id':{$exists:true}
        }
      })
     
      pipelinedata.push({
        $project: {
          EXPORTER_CODE: 1,
        }
      })
      Unassigned.push({
        $project: {
          EXPORTER_CODE: 1,
        }
      })
      const AssignedRes = await ExporterModel.aggregate(pipelinedata)
      const unAssignedRes = await ExporterModel.aggregate(Unassigned)
      resolve({
        success:true,
        message : {
          assigned: AssignedRes,
          unassigned: unAssignedRes
        }
      })
    }catch(e){
      console.log('error in e',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}

function addUniqueElements(arr, elementsToAdd) {
  const uniqueElements = elementsToAdd.filter(element => !arr.includes(element));
  return arr.concat(uniqueElements);
}

exports.AssignMasterBulkDataTask = async(req,res) => {
  try{
    const result =  await  AssignMasterBulkDataTaskFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const AssignMasterBulkDataTaskFunc = ({AssignmentObject, STATUS,FOLDER_NAME,ASSIGNEE_NAME,ASSIGNEE_ID,FILTERS}) => {
  return new Promise(async(resolve,reject) =>{
    try{
      const assignedTo = Object.keys(AssignmentObject)
      const expTo = Object.values(AssignmentObject)
      if(assignedTo.length === 0){
        return reject({
          success:false,
          message:'No data available to assign'
        })
      }
      for(let i=0;i<=assignedTo.length-1;i++){
        resolve({
          success:true,
          message:'Data Assignment started, Please while we are assigning data to users. This may take a while'
        })
        const leadid = assignedTo[i]
        const query = `SELECT tbl_user_id as id,contact_person,name_title,designation,email_id FROM tbl_user_details WHERE ${leadid.includes("(") ? `tbl_user_id IN ${leadid}` : `tbl_user_id = '${leadid}'`}`
        const dbRes = await call({query},'makeQuery','get')
        const LeadAssignedObj = dbRes.message
        const exps = expTo[i]
        const EXPORTER_LIST = exps.EXPORTER_CODE
        const TASK_TYPE = exps.selectedTask
        let updateCount = 0
        let newinsertCount = 0
        let expCodes = []
        for(let j=0 ; j <= EXPORTER_LIST.length - 1 ;j++){
          const EXPORTER_CODE = EXPORTER_LIST[j].EXPORTER_CODE 
          const crmTasks = await ExporterModelV2.find({EXPORTER_CODE:EXPORTER_CODE})
          expCodes.push(EXPORTER_CODE)
          if(crmTasks.length >= 1){
            //Data Already Exists just update the assignee
            try{
              const res = await ExporterModelV2.updateOne({EXPORTER_CODE:EXPORTER_CODE},{TASK_ASSIGNED_TO:LeadAssignedObj,TASK_TYPE:TASK_TYPE,TASK_DATE:null,...(FOLDER_NAME && { FOLDER_NAME: FOLDER_NAME })})
              //console.log('ModifiedCount',res.modifiedCount,res.matchedCount,EXPORTER_CODE);
              updateCount += res.modifiedCount
            }catch(e) {
              console.log('error in ', e);
            }
          }else{
            const exporterlist = await ExporterModelV2.find({EXPORTER_CODE:EXPORTER_CODE})
            if(exporterlist.length >= 1){
              const tommDate =  new Date()
              tommDate.setDate(tommDate.getDate() + 1)
              const exporterObj = exporterlist[0]
              const crmObj = {
                EXPORTER_CODE:exporterObj.EXPORTER_CODE,
                EXPORTER_NAME:exporterObj.EXPORTER_NAME,
                EXPORTER_ADDRESS:exporterObj.EXPORTER_ADDRESS,
                EXPORTER_CITY: exporterObj.EXPORTER_CITY,
                TOTAL_BUYERS:exporterObj.BUYERS.length,
                TOTAL_SHIPMENTS:exporterObj.TOTAL_SHIPMENTS,
                FOB: exporterObj.FOB,
                HS_CODES:exporterObj.HS_CODES,
                EXTRA_DETAILS:[],
                TASK_ASSIGNED_TO:LeadAssignedObj,
                STATUS:STATUS ? STATUS : 0,
                TASK_TYPE,
                FOLDER_NAME:FOLDER_NAME
                //TASK_DATE: new Date(taskDate)
              }
  
              const TOP_COUNTRIES = await TTV.aggregate([
                {
                  '$match': {
                    'EXPORTER_NAME': exporterObj.EXPORTER_NAME
                  }
                }, {
                  '$group': {
                    '_id': '$DESTINATION_COUNTRY', 
                    'total_shipments': {
                      '$sum': 1
                    }, 
                    'FOB': {
                      '$sum': '$FOB_VALUE_USD'
                    }, 
                    'destination_country': {
                      '$first': '$DESTINATION_COUNTRY'
                    }
                  }
                }, {
                  '$sort': {
                    'FOB': -1
                  }
                }, {
                  '$limit': 2
                }
              ])
              crmObj["TOP_COUNTRIES"] = TOP_COUNTRIES
                
              await ExporterModelV2.create(crmObj)
              newinsertCount +=1
            }
          }
        }
        if(FOLDER_NAME){
          const folder = await CRMFolder.find({folderName: FOLDER_NAME})
          if(FOLDER_NAME && folder.length > 0){
            let existingExpCodes = folder[0]["assignedCodes"]
            let updatedExpCodes =addUniqueElements(existingExpCodes,expCodes)
            await CRMFolder.updateOne({folderName:FOLDER_NAME},{$set:{updatedAt:new Date(),updatedBy:ASSIGNEE_NAME,assignedCodes:updatedExpCodes}})
          }else{
            await CRMFolder.create({
              folderName:FOLDER_NAME,
              assignedByName:ASSIGNEE_NAME,
              assignedById:ASSIGNEE_ID,
              assignmentDate: new Date(),
              filters:FILTERS,
              assignedCodes:expCodes,
              updatedAt: new Date(),
              updatedBy : ASSIGNEE_NAME
            })
          }
        }

      }
    }catch(e){
      console.log('error in addmASTERtASK',e);
      reject({
        success:false,
        message:'Failed to assign data'
      })
    }
  })
}


exports.getOverallCRMTasks = async(req,res) => {
  try{
    const result = await getOverallCRMTasksFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getOverallCRMTasksFunc = ({currentPage ,resultPerPage,taskUpdate,search,onlyShowForUserId,included_status,leadAssignedTo,hscodes,leadsStatus,requirements,taskStatus,TasksState,taskType,folderName}) =>{
  return new Promise(async(resolve, reject) => {
    try {
    let FOB_BY_HS = null
    let includedTasks = []
    if(taskUpdate?.includes("User Onboarded")){
      if(taskUpdate && taskUpdate.length == 1){
        includedTasks = [4]
      }else{
        includedTasks.push(4)
      }
    }
    let folderobj = {}
    if(folderName === 'Default'){
      folderobj = {
        'FOLDER_NAME' : {
          $eq : null
        }
      }
    }else if(folderName){
      folderobj = {
        'FOLDER_NAME' : {
          $eq : folderName
        }
      }
    }
    const hs = folderName?.split("HS")?.[1]
    if(!isNaN(parseInt(hs))){
      let actualHS = convertStringToList(hs.trim())

      FOB_BY_HS = {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$HS_CODES",
                as: "code",
                cond: {
                  $in: [
                    { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                    actualHS
                  ]
                }
              }
            },
            as: "code",
            in: "$$code.FOB_VALUE_USD"
          }
        }
      }  
    }
    let mainPipeline = [
      {
        $match :{
          $and: [
            { 'TASK_ASSIGNED_TO.id' : {$exists : true}},
            {STATUS : {$in:[0,1,2,3,4]}},
            folderobj
          ]
        }
      }
    ]
    if(onlyShowForUserId){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.id":onlyShowForUserId
        }
      })
    }
    if(hscodes && hscodes.length){
      const hsCodesRegex = hscodes.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
      mainPipeline.push({
        $match: {
          "HS_CODES.HS_CODES": { $in: hsCodesRegex }
        }
      });
      FOB_BY_HS = {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$HS_CODES",
                as: "code",
                cond: {
                  $in: [
                    { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                    hscodes
                  ]
                }
              }
            },
            as: "code",
            in: "$$code.FOB_VALUE_USD"
          }
        }
      } 
    }
    if(requirements && requirements.length){
      mainPipeline.push({
        $match: {
          'INTRESTED_SERVICES' : {$in : requirements}
        }
      })
      
    }

    if(leadAssignedTo && leadAssignedTo.length){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
        }
      })
    }
    if (search) {
      let matchQuery;
  
      if (!isNaN(search)) { // Check if search is a number
          matchQuery = { 'EXTRA_DETAILS.Contact Number': { $regex: new RegExp(search), $options: 'i' } };
      } else if (/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/.test(search)) { // Check if search is an email address
          matchQuery = { 'EXTRA_DETAILS.Email ID': { $regex: new RegExp(search), $options: 'i' } };
      } else {
          matchQuery = {
              $or: [
                  { EXPORTER_NAME: { $regex: new RegExp(search), $options: 'i' } },
                  { EXPORTER_ADDRESS: { $regex: new RegExp(search), $options: 'i' } }
              ]
          };
      }
  
      mainPipeline.push({
          $match: matchQuery
      });
  }
 
   
    mainPipeline.push({
      $lookup: {
        from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
        localField: 'EXPORTER_CODE',
        foreignField: 'EXPORTER_CODE',
        as: 'task_logs'
      }
    })
    let projectObj = {
      EXPORTER_ADDRESS:1,
      EXPORTER_CITY:1,
      EXPORTER_CODE:1,
      EXPORTER_NAME:1,
      EXTRA_DETAILS:1,
      FOB:1,
      STATUS:1,
      TASK_ASSIGNED_TO:1,
      TOP_COUNTRIES:1,
      TOTAL_BUYERS:1,
      LastNote: {$last: '$task_logs.REMARK'},
      LastEventTime: {$last: '$task_logs.CREATED_AT'},
      LastEventType : {$last: '$task_logs.EVENT_TYPE'},
      LAST_NOTE:1,
      LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
      HS_CODES:1,
      TOTAL_SHIPMENTS:1,
      EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
      TASK_DATE:1,
      task_logs:1,
      EXPORTER_COUNTRY:1

    }
    
    if(FOB_BY_HS){
      projectObj["FOB"] = FOB_BY_HS
    }
    mainPipeline.push({
      $project : projectObj
    })
    if(TasksState && TasksState.length){
      if(TasksState.includes('Task Created') && TasksState.includes('Task Not Created')){
        // mainPipeline.push({
        //   $match: {
        //     'LastNote' : 
        //   }
        // })
      }else if(TasksState.includes('Task Created')){
        mainPipeline.push({
          $match: {
            'LOG_TYPE' : {
              $exists: true
            }
          }
        })
      }else if(TasksState.includes('Task Not Created')){
        mainPipeline.push({
          $match: {
            'LOG_TYPE' : {
              $exists: false
            }
          }
        })
      }
    }
    // mainPipeline.push({
    //   $sort : {
    //     'TASK_DATE' : 1
    //   } 
    // })
    if(taskStatus && taskStatus.length){
      mainPipeline.push({
        $match: {
          'EVENT_STATUS' : {
            $in : taskStatus.map(item => new RegExp(item))
          }
        }
      })
    } 
    if(leadsStatus && leadsStatus.length){
      if(leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': [0,1,2,3,4]
            }
          }
        })
      }else if(leadsStatus.includes("Lead Created")){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': [1]
            }
          }
        })
      }else if(leadsStatus.includes("Lead Not Created")){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': [0,2,3,4]
            }
          }
        })
      }
    }
    if(taskUpdate){
      let statusArray = taskUpdate.filter(element => element !== 'User Onboarded' && element !== 'Lead Created')
      if(statusArray && statusArray.length ){
        mainPipeline.push({
          $match:{
            $or : [
              {
                'STATUS': {
                  '$in': includedTasks
                }
              },
              {$and : [
                {'LOG_TYPE' : 'Didnt connect'},
                {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
              ]
              },
              {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
            ]
          }
        })
      }else{
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': includedTasks
            }
          }
        })
      }
        // mainPipeline.push({
        //   $match:{
        //     $or : [
        //       {
        //         'STATUS': {
        //           '$in': includedTasks
        //         }
        //       },
        //       statusArray && statusArray.length ? {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }} : {}
        //     ]
        //   }
        // })
      
    }else{
      if(!leadsStatus){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': included_status
            }
          }
        })
      }
    }
    
    const countpipeline = [...mainPipeline]
    let countoptimized = [...countpipeline]
    if(!(taskStatus || taskUpdate || TasksState)){
      countoptimized = countpipeline.filter((stage) => !("$lookup" in stage))
    }
    countoptimized.push({
      '$count': 'total_records'
    })
    const countRes = await ExporterModelV2.aggregate(countoptimized)
    const total_records = countRes[0]?.total_records
    if(currentPage && resultPerPage) {
      mainPipeline.push({
        '$skip': (currentPage - 1) * parseInt(resultPerPage) 
      })
      mainPipeline.push({
        '$limit': parseInt(resultPerPage) 
      })
    }  

    if(taskType === 'Corporate'){
      mainPipeline.push({
        $lookup: {
          from: env === 'dev' ? 'tbl_crm_applications' : 'tbl_crm_applications_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'crm_applications'
        }
      })
    }
      //const response = await ExporterModelV2.aggregate(mainPipeline)
      // const response = await ExporterModelV2.find({
      //   $expr: {
      //     $eq: [
      //       { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
      //       dateRangeFilter && dateRangeFilter[0] ? dateRangeFilter[0] : moment().format('YYYY-MM-DD') // compare with the target date string
      //     ]
      //   }
      // }).skip((currentPage - 1 ) * resultPerPage).limit(resultPerPage)
      mainPipeline.push({
        $sort: {
          'FOB':-1
        }
      })
      console.log(JSON.stringify(mainPipeline))
      const response = await ExporterModelV2.aggregate(mainPipeline)
      resolve({
        success:true,
        message:{
          message:response,
          total_records
        }
      })

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}

exports.getOverallCRMTasksFilters = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filterData = {}
    const { dateRangeFilter,taskStatus,included_status,leadAssignedTo,onlyShowForUserId }  = req.body
    filterData["Task"] = {
      "accordianId":"TasksState",
      type: "checkbox",
      labelName: "name",
      data:[{name:"Task Created"}, {name:"Task Not Created"}]
    }
    filterData["Task Update"] = {
      "accordianId": 'taskUpdate',
      type: "checkbox",
      labelName: "name"
    }
    filterData["Task Update"]["data"] = [{"name" : "Task"},{"name" : "Didnt connect"} ,{ name: "Busy"},
    { name: "Not Reachable"},
    { name: "Wrong Number" },
    { name: "Invalid Number" },
    { name: "Switched Off"},{"name" : "Call back"}  ,{"name" : "Not Interested"}, {"name":"Lead Lost"},{name: "User Onboarded"}]
    if(!reqBody.onlyShowForUserId){
      filterData["Lead Assigned To"] = {
        "accordianId": 'leadAssignedTo',
        type: "checkbox",
        labelName: "name"
      }
      let query = `SELECT tbl_user_details.contact_person AS name FROM tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user.id = tbl_user_details.tbl_user_id WHERE tbl_user.isSubUser = 1 AND tbl_user.type_id = 1 `
      let dbRes = await call({ query }, 'makeQuery', 'get');
      filterData["Lead Assigned To"]["data"] = dbRes.message
    }

    filterData["Leads"] = {
      "accordianId": 'leadsStatus',
      type: "checkbox",
      labelName: "name",
      data: [{name:"Lead Created"}, {name:"Lead Not Created"}]
    }
    filterData["Status"] = {
      "accordianId": 'taskStatus',
      type: "checkbox",
      labelName: "name",
      data:  [{ name: "Hot"}, {name: "Cold"}, {name: "Warm" }]
    }
    filterData["Requirement"] = {
      "accordianId": 'requirements',
      type: "checkbox",
      labelName: "name",
      data:  [
        { name: "Export LC discounting" },
        { name: "Export LC confirmation" },
        { name: "Import LC discounting" },
        { name: "Export invoice discounting" },
        { name: "SBLC" },
        { name: "Supply chain finance" },
        { name: "Import factoring" },
        { name: "Usance at sight" },
        { name: "Freight finance" },
        { name: "Packing credit" },
        { name: "Purchase order financing   " },
        { name: "Reverse factoring" },
        { name: "Trade credit insurance" }
      ]
    }
    const query = `SELECT code as name FROM tbl_hsn_codes`
    const dbRes = await call({query},'makeQuery','get')
    filterData["HS Code"] = {
      "accordianId": 'hscodes',
      type: "checkbox",
      labelName: "name",
      data:  dbRes.message
    }

    // let matchobj  = {}
    // if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
    //   matchobj = {
    //     $expr: {
    //       $eq: [
    //         { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
    //           dateRangeFilter?.[0]  // compare with the target date string
    //       ]
    //     }
    //   }
         
    // }else{
    //   matchobj = {
    //     'TASK_DATE' :{
    //       $gte: new Date(dateRangeFilter?.[0]),
    //       $lte: new Date(dateRangeFilter?.[1])
    //      }
    //   }
    // }
    // let mainPipeline = [
    //   {
    //     $match :{STATUS : {$in:included_status}}
    //   },
    //   { 
    //     $match : matchobj
    //   }
    // ]
    // if(onlyShowForUserId){
    //   mainPipeline.push({
    //     $match: {
    //       "TASK_ASSIGNED_TO.id":onlyShowForUserId
    //     }
    //   })
    // }
    // if(leadAssignedTo && leadAssignedTo.length){
    //   mainPipeline.push({
    //     $match: {
    //       "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
    //     }
    //   })
    // }
    // mainPipeline.push({
    //   $sort : {
    //     'FOB' : -1
    //   } 
    // })
    // mainPipeline.push({
    //   $lookup: {
    //     from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
    //     localField: 'EXPORTER_CODE',
    //     foreignField: 'EXPORTER_CODE',
    //     as: 'task_logs'
    //   }
    // })
    // mainPipeline.push({
    //   $project : {
    //     EXPORTER_NAME:1,
    //     EXTRA_DETAILS:1,
    //     LOG_TYPE: {$first: '$task_logs.LOG_TYPE'}
    //   }
    // })
    // if(taskStatus){
    //   mainPipeline.push({
    //     $match:{
    //       LOG_TYPE: {$in: taskStatus.map(item => new RegExp(item)) }
    //     }
    //   })
    // }
    // const extradetailsPipeline = [...mainPipeline,{
    //   $unwind:"$EXTRA_DETAILS"
    // },
    // {
    //   $group: {
    //     _id: null,
    //     'Contact_Person': {
    //       $addToSet : {
    //         name:'$EXTRA_DETAILS.Contact Person'
    //       }
    //     },
    //     'Contact_Number': {
    //       $addToSet : {
    //         name:'$EXTRA_DETAILS.Contact Number'
    //       }
    //     },
    //      'Designation': {
    //       $addToSet : {
    //         name:'$EXTRA_DETAILS.Designation'
    //       }
    //     }
    //   }
    // }
    // ]
    // const exporterNamePipeline = [...mainPipeline,{
    //   $group : {
    //     '_id': null,
    //     'EXPORTER_NAME':{
    //       '$addToSet': {
    //         'name' : '$EXPORTER_NAME'
    //       }
    //     }
    //   }
    // }]
    // const extradetailsResponse = await ExporterModelV2.aggregate(extradetailsPipeline)
    // const exporterNameResponse = await ExporterModelV2.aggregate(exporterNamePipeline)
    // filterData["Company Name"] = {
    //   "accordianId": 'companyName',
    //   type: "checkbox",
    //   labelName: "name",
    //   data : exporterNameResponse?.[0]?.EXPORTER_NAME
    // }


    // filterData["Contact No"] = {
    //   "accordianId": 'contactNo',
    //   type: "checkbox",
    //   labelName: "name",
    //   data: extradetailsResponse?.[0]?.Contact_Number
    // }

    // filterData["Contact Person"] = {
    //   "accordianId": 'contactPerson',
    //   type: "checkbox",
    //   labelName: "name",
    //   data: extradetailsResponse?.[0]?.Contact_Person
    // }

    // filterData["Designation"] = {
    //   "accordianId": 'designation',
    //   type: "checkbox",
    //   labelName: "name",
    //   data: extradetailsResponse?.[0]?.Designation
    // }
    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("error in getEnquiryAdminFilters", error);
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getOverallCRMStats = async(req,res) => {
  try{
    const result = await getOverallCRMStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getOverallCRMStatsFunc = ({taskUpdate,search,dateRangeFilter,onlyShowForUserId,leadAssignedTo,hscodes,leadsStatus,requirements,taskStatus,included_status,taskType,folderName,screen_name}) =>{
  return new Promise(async(resolve,reject)=> {
    try{
      let matchobj  = {}
      let tasksmatchObj ={ }
      let folderobj = {}
      if(folderName === 'Default'){
        folderobj = {
          'FOLDER_NAME' : {
            $eq : null
          }
        }
      }else if(folderName){
        folderobj = {
          'FOLDER_NAME' : {
            $eq : folderName
          }
        }
      }
      if(dateRangeFilter && dateRangeFilter.length >=1){
        if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
          matchobj = {
            $expr: {
              $eq: [
                { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
                  dateRangeFilter?.[0]  // compare with the target date string
              ]
            }
          }
             
        }else{
          matchobj = {
            'TASK_DATE' :{
              $gte: new Date(dateRangeFilter?.[0]),
              $lte: new Date(dateRangeFilter?.[1])
             }
          }
        }
      }
      let mainPipeline = [
      { 
        $match : matchobj
      },
      {
        $match : {
          'TASK_ASSIGNED_TO' : {$exists : true},
          ...folderobj
        }
      }
      ]
      if(onlyShowForUserId){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.id":onlyShowForUserId
          }
        })
      }
      if(leadAssignedTo && leadAssignedTo.length){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
          }
        })
      }
      const leadsPipeline = [...mainPipeline]
      const onboardPipeline  = [...mainPipeline]

      let includedTasks = []
      if(taskUpdate?.includes("User Onboarded")){
        if(taskUpdate && taskUpdate.length == 1){
          includedTasks = [4]
        }else{
          includedTasks.push(4)
        }
      }
      if(hscodes && hscodes.length){
        const hsCodesRegex = hscodes.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
        mainPipeline.push({
          $match: {
            "HS_CODES.HS_CODES": { $in: hsCodesRegex }
          }
        });
      }
      if(requirements && requirements.length){
        mainPipeline.push({
          $match: {
            'INTRESTED_SERVICES' : {$in : requirements}
          }
        })
        
      }
  
    
      if(search){
        mainPipeline.push({
          $match:{
            EXPORTER_NAME: {$regex: new RegExp(search) , $options:'i'}
          }
        })
      }
      mainPipeline.push({
        $lookup : {
          from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'task_logs'
        }
      })
      let  pendingPipeline =  mainPipeline
      pendingPipeline = [...pendingPipeline]
      if(taskType === 'Exporter Wise'){
        pendingPipeline.push({
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            },
            STATUS:1,
            TASK_ASSIGNED_TO:1,
            LAST_NOTE:1,
            LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
            EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
            TASK_DATE:1,
            EVENT_TIME:{$last: '$task_logs.EVENT_TIME'},
            EXPORTER_CODE:1
          }
        })
      }
      
      mainPipeline.push(
        taskType === 'Exporter Wise'? {
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            },
            STATUS:1,
            TASK_ASSIGNED_TO:1,
            LAST_NOTE:1,
            LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
            EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
            TASK_DATE:1,
            EVENT_TIME:{$last: '$task_logs.EVENT_TIME'},
            EXPORTER_CODE:1
          }
        } :{
          '$unwind': {
              'path': '$task_logs', 
              'includeArrayIndex': 'i', 
              'preserveNullAndEmptyArrays': true
          }
        },
      )
      if(taskType === 'Task Wise'){
        mainPipeline.push({
          $project: {
            EVENT_STATUS : '$task_logs.EVENT_STATUS',
            STATUS : 1,
            LOG_TYPE:'$task_logs.LOG_TYPE',
            TASK_DATE:1 ,
            EVENT_TIME:'$task_logs.EVENT_TIME',
            EXPORTER_CODE:1
          }
        })
      }
      if(taskStatus && taskStatus.length){
        mainPipeline.push({
          $match: {
            'EVENT_STATUS' : {
              $in : taskStatus.map(item => new RegExp(item))
            }
          }
        })
        if(taskType === 'Exporter Wise'){
          pendingPipeline.push({
            $match: {
              'EVENT_STATUS' : {
                $in : taskStatus.map(item => new RegExp(item))
              }
            }
          })
        }else{
          pendingPipeline.push({
            $match: {
              '$task_logs.EVENT_STATUS' : {
                $in : taskStatus.map(item => new RegExp(item))
              }
            }
          })
        }
      } 
      if(leadsStatus && leadsStatus.length){
        if(leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
        }
      }
      if(taskUpdate){
        let statusArray = taskUpdate.filter(element => element !== 'User Onboarded' && element !== 'Lead Created')
        if(statusArray && statusArray.length ){
          mainPipeline.push({
            $match:{
              $or : [
                {
                  'STATUS': {
                    '$in': includedTasks
                  }
                },
                {$and : [
                  {'LOG_TYPE' : 'Didnt connect'},
                  {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
                },
                {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
              ]
            }
          })
          if(taskType === 'Exporter Wise'){
            pendingPipeline.push({
              $match:{
                $or : [
                  {
                    'STATUS': {
                      '$in': includedTasks
                    }
                  },
                  {$and : [
                    {'LOG_TYPE' : 'Didnt connect'},
                    {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                  ]
                  },
                  {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
                ]
              }
            })
          }else{
            pendingPipeline.push({
              $match:{
                $or : [
                  {
                    'STATUS': {
                      '$in': includedTasks
                    }
                  },
                  {$and : [
                    {'$task_logs.LOG_TYPE' : 'Didnt connect'},
                    {'$task_logs.EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                  ]
                  },
                  {'$task_logs.LOG_TYPE': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
              }
            })
          }
        }else{
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
        }
          // mainPipeline.push({
          //   $match:{
          //     $or : [
          //       {
          //         'STATUS': {
          //           '$in': includedTasks
          //         }
          //       },
          //       statusArray && statusArray.length ? {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }} : {}
          //     ]
          //   }
          // })
        
      }else{
        if(!leadsStatus){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
        }
      }
  
      const tasksOverallPipeline =  [ ...mainPipeline,   
       taskType === 'Exporter Wise'? {
        '$group': {
          '_id': null, 
          'tasksFollowup': {
            '$sum': {
              '$cond': [
                {
                  '$in': [
                    '$LOG_TYPE', [
                      'Call back', 'Didnt connect','Create New Task'
                    ]
                  ]
                }, 1, 0
              ]
            }
          }, 
          'tasksNew': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    { '$type': '$LOG_TYPE' },
                    'missing'
                  ]
                }, 1, 0
              ]
            }
          }
        }
      } : {
        '$group': {
          '_id': null, 
          'tasksFollowup': {
            '$sum': {
              '$cond': [
                {
                  '$in': [
                    '$LOG_TYPE', [
                      'Call back', 'Didnt connect','Create New Task'
                    ]
                  ]
                }, 1, 0
              ]
            }
          }, 
          'tasksNew': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    { '$type': '$LOG_TYPE' },
                    'missing'
                  ]
                }, 1, 0
              ]
            }
          }
        }
      }
      ]
      leadsPipeline.push({
        $match:{
          STATUS:1
        }
      })
      leadsPipeline.push({
        $count : 'total_records'
      })
      onboardPipeline.push({
        $match:{
          STATUS:4
        }
      })
      onboardPipeline.push({
        $count : 'total_records'
      })
      const logTypePipeline = [...mainPipeline]
      // logTypePipeline.push({
      //   $match:{
      //     STATUS:{$in : [0,1,2,3]}
      //   }
      // })
      
      logTypePipeline.push({
        $group : {
          _id: '$LOG_TYPE',
          'total_records' : {$sum: 1},
          'LOG_TYPE':{$first:'$LOG_TYPE'}
        }
      })
      mainPipeline.push({
        $match : {
          EVENT_STATUS: {
            $in: [
              "Hot (30 days or less)",
              "Cold (60 days or more)",
              "Warm (30-60 days)"
            ]
          }
        }
      })
      mainPipeline.push({
        $group : {
          _id: '$EVENT_STATUS',
          'total_records' : {$sum: 1},
          'EVENT_TYPE':{$first : '$EVENT_STATUS'}
        }
      })
      let tasksInComplete = 0
      let tasksCompleted = 0

      const eventResponse = await ExporterModelV2.aggregate(mainPipeline)
      const logsResponse = await ExporterModelV2.aggregate(logTypePipeline)
      const tasksOverallResponse  = await ExporterModelV2.aggregate(tasksOverallPipeline)

      if(screen_name === 'CRM List'){
        const pendingResponse = await ExporterModelV2.aggregate(pendingPipeline)
        for(let i=0; i<= pendingResponse.length - 1 ; i++){
          const element = pendingResponse[i]
          if(taskType === 'Exporter Wise'){
            if(element.LOG_TYPE === undefined){
              tasksInComplete += 1
            }else{
              const TasksLogs = element.task_logs
              if(TasksLogs.LOG_TYPE === 'Lead Lost' || TasksLogs.LOG_TYPE === 'User Onboarded' || TasksLogs.LOG_TYPE === 'Not Interested' || TasksLogs.LOG_TYPE === 'Didnt connect'){
                tasksCompleted += 1
              }
              else if((new Date(TasksLogs.EVENT_TIME).getTime() <= new Date(dateRangeFilter[0]).getTime() && (new Date(TasksLogs.EVENT_TIME).getTime() >= new Date(dateRangeFilter[1]).getTime()))){
                tasksCompleted += 1
              }else {
                tasksInComplete += 1
              }
            }
          }else{
            if(element.task_logs === undefined || element?.task_logs?.length === 0){
              tasksInComplete += 1
            }else{
              for(let j = 0; j<= element.task_logs.length - 1 ; j++){
                const item = element.task_logs[j]
                if(!dateRangeFilter){
                  tasksCompleted += 1
                }
                else if(item.LOG_TYPE === 'Lead Lost' || item.LOG_TYPE === 'User Onboarded'|| item.LOG_TYPE === 'Not Interested' || item.LOG_TYPE === 'Didnt connect'){
                  tasksCompleted += 1
                }else if((new Date(item.EVENT_TIME).getTime() <= new Date(dateRangeFilter[0]).getTime()) && (new Date(item.EVENT_TIME).getTime() >= new Date(dateRangeFilter[1]).getTime())){
                    if(element.task_logs[j+1]){
                    tasksCompleted += 1
                  }else{
                    tasksCompleted += 1
                  }
                }else{
                  tasksInComplete +=1
                }
              }
            }
            
          }
        }
      }

      resolve({
        success:true,
        message:{
          eventResponse,
          logsResponse,
          leadsCount : logsResponse?.filter(item => item.LOG_TYPE === 'Lead Created')?.[0]?.total_records,
          onboardCount :logsResponse?.filter(item => item.LOG_TYPE === 'User Onboarded')?.[0]?.total_records,
          pendingCount :tasksInComplete,
          completedCount:tasksCompleted,
          newTaskCount : tasksOverallResponse?.[0]?.tasksNew,
          FollowupCount : tasksOverallResponse?.[0]?.tasksFollowup,
        }
      })
    }catch(e){
      console.log('error in apio',e);
      reject({
        success:false
      })
    }
  })
}

exports.getBuyerListCRM = async (req,res) => {
  try{
    const result = await getBuyerListCRMFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getBuyerListCRMFunc = async ({ttvExporterCode,buyers,resultPerPage,currentPage,search}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let searchObj = {}
      if(search){
        searchObj = {
          'CONSIGNEE_NAME': {$regex: new RegExp(search), $options:'i'}
        }
      }
      let buyerdata = []
      if(!buyers){
        let exporterdata = await ExporterModel.find({EXPORTER_CODE:ttvExporterCode})
        buyerdata = exporterdata[0].BUYERS
      }
      const mainpipeline = [
        {
          '$match': {
            '$and': [
              {
                'CONSIGNEE_CODE': {
                  '$in': buyers ? buyers: buyerdata
                }
              }, {
                'EXPORTER_CODE': ttvExporterCode
              },
              searchObj
            ]
          }
        }, {
          '$group': {
            '_id': '$CONSIGNEE_CODE', 
            'TOTAL_SHIPMENTS': {
              '$sum': 1
            }, 
            'DESTINATION_COUNTRY': {
              '$first': '$DESTINATION_COUNTRY'
            }, 
            'FOB': {
              '$sum': '$FOB_VALUE_USD'
            }, 
            'HSN_CODES': {
              '$addToSet': '$HS_CODE'
            }, 
            'PRODUCT_TYPE': {
              '$addToSet': '$PRODUCT_TYPE'
            },
            'CONSIGNEE_NAME':{$first:"$CONSIGNEE_NAME"}
          }
        }, {
          '$sort': {
            'FOB': -1
          }
        }
      ]
      const countPipeline = [...mainpipeline]
      countPipeline.push({
        $count:'total_records'
      })
      
      if(currentPage && resultPerPage) {
        mainpipeline.push({
          '$skip': (currentPage - 1) * parseInt(resultPerPage) 
        })
        mainpipeline.push({
          '$limit': parseInt(resultPerPage) 
        })
      }
      const response = await TTV.aggregate(mainpipeline)
      const countRes = await TTV.aggregate(countPipeline)
      resolve({
        success:true,
        message:{
          message:response,
          total_records: countRes?.[0]?.total_records || 0
        }
      })
    }catch(e){
      console.log('error in buyers API',e);
      reject({
        success:false,
        message:''
      })
    }
  })
  
}

exports.getTasksListCRM = async (req,res) => {
  try{
    const result = await getTasksListCRMFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getTasksListCRMFunc = async ({ttvExporterCode,resultPerPage,currentPage}) => {
  return new Promise(async(resolve,reject) => {
    try{
      
      const mainpipeline = [
        {
          '$match': {
            'EXPORTER_CODE': ttvExporterCode
          }
        }, {
          '$lookup': {
            'from': env === 'dev' ? 'india_export_exporters_list' : 'india_export_exporters_list_prod', 
            'localField': 'EXPORTER_CODE', 
            'foreignField': 'EXPORTER_CODE', 
            'as': 'crm_tasks'
          }
        }, {
          '$project': {
            'EVENT_TIME': 1, 
            'EVENT_STATUS': 1, 
            'LOG_TYPE': 1, 
            'CREATOR': {
              '$first': '$crm_tasks.TASK_ASSIGNED_TO'
            }, 
            'REMARK': 1, 
            'EXPORTER_NAME': 1,
            'EVENT_TYPE':1
          }
        }
      ]
      const countPipeline = [...mainpipeline]
      countPipeline.push({
        $count:'total_records'
      })
      
      if(currentPage && resultPerPage) {
        mainpipeline.push({
          '$skip': (currentPage - 1) * parseInt(resultPerPage) 
        })
        mainpipeline.push({
          '$limit': parseInt(resultPerPage) 
        })
      }
      const response = await CRMTasksLogs.aggregate(mainpipeline)
      const countRes = await CRMTasksLogs.aggregate(countPipeline)
      resolve({
        success:true,
        message:{
          message:response,
          total_records: countRes?.[0]?.total_records || 0
        }
      })
    }catch(e){
      console.log('error in buyers API',e);
      reject({
        success:false,
        message:''
      })
    }
  })
  
}

exports.getCRMMasterdataFilters = async (req,res) => {
  try{
    const result = await getCRMMasterdataFiltersFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCRMMasterdataFiltersFunc = async ({ country_name, searchParam, HS_CODES, EXPORTER_CODES }) => {
 return new Promise(async(resolve,reject) => {
  try {
    const pipelinedata = [];
    const reddisVariable = `${country_name}${searchParam}${HS_CODES.join(",")}`
    const cachedData = await redisInstance.redisGetSync(reddisVariable)
    if(cachedData){
      return resolve({
        success: true,
        message: JSON.parse(cachedData)
      })
    }
    const searchRegex = new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`);
    const searchObj = isNaN(parseInt(searchParam))
      ? { 'EXPORTER_NAME': { $regex: new RegExp(`${searchParam}`, 'i') } }
      : { "HS_CODES.HS_CODES": { $regex: searchRegex } };

    const ttvSearchObj = { ...searchObj, 'EXPORTER_NAME': { $regex: new RegExp(`${searchParam}`, 'i') } };
    const hsObj = HS_CODES && HS_CODES.length
      ? { "HS_CODES.HS_CODES": { $in: HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`)) } }
      : {};

    const ttvHSObj = HS_CODES && HS_CODES.length
      ? { "HS_CODE": { $in: HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`)) } }
      : {};

    pipelinedata.push({
      $match: {
        $and: [
          searchObj,
          { 'EXPORTER_COUNTRY': { $regex: new RegExp(country_name, 'i') } },
          EXPORTER_CODES ? {'EXPORTER_CODE': {$in: EXPORTER_CODES}} :{},
          hsObj
        ]
      }
    });

    const response = await ExporterModel.aggregate([
      ...pipelinedata,
      {
        $group: {
          '_id': null,
          'EXPORTER_CITY': { '$addToSet': '$EXPORTER_CITY' },
          'EXPORTER_COUNT': { '$sum': 1 }
        }
      }
    ]);

    pipelinedata.push({
      $lookup: {
        from: env === 'dev' ? 'tbl_crm_tasks' : 'tbl_crm_tasks_prod',
        localField: 'EXPORTER_CODE',
        foreignField: 'EXPORTER_CODE',
        as: 'crm_tasks'
      }
    });

    pipelinedata.push({
      $project: {
        'EXPORTER_NAME': 1,
        'EXPORTER_CITY': 1,
        'EXTRA_DETAILS': { $ifNull: [{ $first: '$crm_tasks.EXTRA_DETAILS' }, []] }
      }
    });

    const countPipeline = (matchObj) => [
      ...pipelinedata,
      { $match: matchObj },
      { $count: 'count' }
    ];

    const [emailRes, contactRes, bothRes, NotbothRes] = await Promise.all([
      ExporterModel.aggregate(countPipeline({
        "EXTRA_DETAILS.Contact Number": { $exists: false },
        "EXTRA_DETAILS.Email ID": { $exists: true }
      })),
      ExporterModel.aggregate(countPipeline({
        "EXTRA_DETAILS.Contact Number": { $exists: true },
        "EXTRA_DETAILS.Email ID": { $exists: false }
      })),
      ExporterModel.aggregate(countPipeline({
        "EXTRA_DETAILS.Contact Number": { $exists: true },
        "EXTRA_DETAILS.Email ID": { $exists: true }
      })),
      ExporterModel.aggregate(countPipeline({
        "EXTRA_DETAILS.Contact Number": { $exists: false },
        "EXTRA_DETAILS.Email ID": { $exists: false }
      }))
    ]);

    const buyerspipeline = [
      { $match: { $and: [ttvSearchObj, ttvHSObj] } },
      {
        $group: {
          _id: '$CONSIGNEE_CODE',
          BUYER_NAME: { $first: '$CONSIGNEE_NAME' },
          BUYER_CODE: { $first: '$CONSIGNEE_CODE' }
        }
      },
      { $sort: { CONSIGNEE_NAME: 1 } },
      { $project: { _id: 0 } }
    ];

    const countriespipeline = [
      { $match: { $and: [ttvSearchObj, ttvHSObj] } },
      {
        $group: {
          _id: '$DESTINATION_COUNTRY',
          DESTINATION_COUNTRY: { $first: '$DESTINATION_COUNTRY' }
        }
      },
      { $sort: { DESTINATION_COUNTRY: 1 } },
      { $project: { _id: 0 } }
    ];

    const [buyersResponse, countriesResponse] = await Promise.all([
      TTVSummary.aggregate(buyerspipeline),
      TTVSummary.aggregate(countriespipeline)
    ]);
    const finalRes = {
      ...response?.[0],
      email_count: emailRes?.[0]?.count || 0,
      contact_count: contactRes?.[0]?.count || 0,
      both_count: bothRes?.[0]?.count || 0,
      both_not: NotbothRes?.[0]?.count || 0,
      EXPORT_COUNTRIES: countriesResponse.map(item => item.DESTINATION_COUNTRY),
      BUYER_NAMES: buyersResponse
    }
    await redisInstance.redisSetSync(reddisVariable,JSON.stringify(finalRes))
    return resolve({
      success: true,
      message: finalRes
    });
  } catch (e) {
    console.log('error in e', e);
    resolve( {
      success: false,
      message: 'Failed to fetch records'
    });
  }
 })
};


exports.getCRMMasterdataFiltersV2 = async (req,res) => {
  try{
    const result = await getCRMMasterdataFiltersFuncV2(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCRMMasterdataFiltersFuncV2 = async ({ country_name, searchParam, HS_CODES, showImports }) => {
 return new Promise(async(resolve,reject) => {
  try {
    const pipelinedata = [];
    const reddisVariable = `${showImports}-${country_name}${searchParam}${HS_CODES.join(",")}`
    const cachedData = await redisInstance.redisGetSync(reddisVariable)
    if(cachedData){
      return resolve({
        success: true,
        message: JSON.parse(cachedData)
      })
    }
    const searchRegex = new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`);
    const searchObj = isNaN(parseInt(searchParam))
    ? { [showImports ? 'EXPORTER_NAME' : 'BUYER_NAME']: { $regex: new RegExp(`${searchParam}`, 'i') } }
      : { "HS_CODES.HS_CODES": { $regex: searchRegex } };

    // const ttvSearchObj = { ...searchObj, 'EXPORTER_NAME': { $regex: new RegExp(`${searchParam}`, 'i') } };

    const ttvSearchObj = isNaN(parseInt(searchParam))
    ? { [showImports ? 'EXPORTER_NAME' : 'CONSIGNEE_NAME']: { $regex: new RegExp(`${searchParam}`, 'i') } }
    : { "HS_CODE": { $regex: searchRegex } };

    const hsObj = HS_CODES && HS_CODES.length
      ? { "HS_CODES.HS_CODES": { $in: HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`)) } }
      : {};

    const ttvHSObj = HS_CODES && HS_CODES.length
      ? { "HS_CODE": { $in: HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`)) } }
      : {};

    pipelinedata.push({
      $match: {
        $and: [
          searchObj,
          { [showImports ? 'EXPORTER_COUNTRY' : 'BUYER_COUNTRY']: { $regex: new RegExp(country_name, 'i') } },
          hsObj
        ]
      }
    });

    let response = null;

    
    if(showImports){
      response = await ExporterModelV2.aggregate([
        ...pipelinedata,
        {
          $group: {
            '_id': null,
            ['EXPORTER_CITY']: { '$addToSet': '$EXPORTER_CITY'},
            ['EXPORTER_COUNT']: { '$sum': 1 }
          }
        }
      ]);
    }
    else{
      response = await BuyerModelV2.aggregate([
        ...pipelinedata,
        {
          $group: {
            '_id': null,
            ['BUYER_CITY']: { '$addToSet': '$BUYER_CITY' },
            ['CONSIGNEE_COUNT']: { '$sum': 1 }
          }
        }
      ]);
    }  

    // console.log("responseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", response);

    const buyerspipeline = showImports ? [
      { $match: { $and: [ttvSearchObj, ttvHSObj] } },
      {
        $group: {
          _id: { 'CONSIGNEE_NAME': '$CONSIGNEE_NAME'},
          'CONSIGNEE_CODE': {'$first': '$CONSIGNEE_CODE'}
        }
      },
      { $project: { _id: 0,
        "CONSIGNEE_NAME": '$_id.CONSIGNEE_NAME',
        "CONSIGNEE_CODE": 1
      }},
      { $sort: { "CONSIGNEE_NAME": 1 } },
    ] : [
      { $match: { $and: [ttvSearchObj, ttvHSObj] } },
      {
        $group: {
          _id: { 'EXPORTER_NAME': '$EXPORTER_NAME'},
          'EXPORTER_CODE': {'$first': '$EXPORTER_CODE'}
        }
      },
      { $project: { _id: 0,
        "EXPORTER_NAME": '$_id.EXPORTER_NAME',
        "EXPORTER_CODE": 1
      }},
      { $sort: { "EXPORTER_NAME": 1 } },
    ];

    const countriespipeline = [
      { $match: { $and: [ttvSearchObj, ttvHSObj] } },
      {
        $group: {
          _id: [showImports ? '$EXPORTER_COUNTRY' : '$DESTINATION_COUNTRY'],
          [showImports ? "EXPORTER_COUNTRY" : "DESTINATION_COUNTRY"]: { $first: showImports ? '$EXPORTER_COUNTRY' : '$DESTINATION_COUNTRY' }
        }
      },
      { $sort: { [showImports ? "EXPORTER_COUNTRY" : "DESTINATION_COUNTRY"]: 1 } },
      { $project: { _id: 0 } }
    ];

    // console.log("matchqyeryyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", ttvSearchObj, ttvHSObj );

    const [buyersResponse, countriesResponse] = await Promise.all([
      TTVSummaryV2.aggregate(buyerspipeline),
      TTVSummaryV2.aggregate(countriespipeline)
    ]);
    // console.log("responseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", countriesResponse);
    const finalRes = {
      ...response?.[0],
      [showImports ? "IMPORT_COUNTRIES" : "EXPORT_COUNTRIES"]: showImports ? countriesResponse.map(item => item.EXPORTER_COUNTRY) : countriesResponse.map(item => item.DESTINATION_COUNTRY),
      [showImports ? "CONSIGNEE_NAMES" : "EXPORTER_NAMES"]: buyersResponse
    }
    await redisInstance.redisSetSync(reddisVariable,JSON.stringify(finalRes))
    return resolve({
      success: true,
      message: finalRes
    });
  } catch (e) {
    console.log('error in e', e);
    resolve( {
      success: false,
      message: 'Failed to fetch records'
    });
  }
 })
};


exports.getCommoditiesCount = async (req,res) => {
  try{
    const result = await getCommoditiesCountFunc(req.body)
    res.send(result)
  }catch(e){
    console.log('error in ')
    res.send(e)
  }
}

const getCommoditiesCountFunc = ({country_name,searchParam,HS_CODES,AVAILABLE_CONTACTS,TURNOVER_RANGE,CITIES,STATUS,ORGANIZATION_TYPE}) => {
  return new Promise(async (resolve,reject) => {
    try{
      const pipelinedata = []
      let searchObj = {}
      let extrasearchobj ={}
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        extrasearchobj = {
          '$match': {
            '_id.hs_code': {
              '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)
            }
          }
        }
      }
      let hsObj ={}
      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item}`))}          
        }
        extrasearchobj = {
          '$match': {
            '_id.hs_code': { '$in': HS_CODES.map(item => new RegExp(`^${item}`))} 
          }
        }
      }
     
      pipelinedata.push({
        $match : {
          $and : [
            searchObj,
            {
              'EXPORTER_COUNTRY': {
                $regex: new RegExp(country_name),
                $options:'i'
              }
            },
            hsObj
          ]
        }
      })
      if(ORGANIZATION_TYPE && ORGANIZATION_TYPE.length){
        pipelinedata.push({
          $match :{
            EXPORTER_NAME: {
              $regex : ORGANIZATION_TYPE.join("|"),
              $options: "i"
            }
          }
        })
      }
     
      if(CITIES && CITIES.length){
        pipelinedata.push({
          $match:{
            'EXPORTER_CITY' : {$in : CITIES }
          }
        })
      }
      if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
        let newObj = []
        for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
          const element = TURNOVER_RANGE[i]
          if(element.minVal && element.maxVal){
            newObj.push({
              'FOB' : {
                $lte:element.minVal,
                $lte:element.maxVal
              }
            })
          }else{
            newObj.push({
              'FOB':{
                $gte:element.maxVal
              }
            })
          }
        }
        pipelinedata.push({
          $match:{
            $or : newObj
          }
        })
      }
      pipelinedata.push({
        $sort:{
          'FOB': -1
        }
      })
      pipelinedata.push({
        $lookup:{
          from: env === 'dev' ? 'tbl_crm_tasks' : 'tbl_crm_tasks_prod',
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'crm_tasks'
        }
      })
      pipelinedata.push({
        $project: {
          EXPORTER_NAME: 1,
          EXPORTER_ADDRESS: 1,
          FOB: 1,
          EXPORTER_CODE: 1,
          EXPORTER_CITY: 1,
          EXTRA_DETAILS: {$first: '$crm_tasks.EXTRA_DETAILS'},
          TASK_ASSIGNED_TO:{$first:"$crm_tasks.TASK_ASSIGNED_TO"},
          TOTAL_BUYERS: {
            $size: {
               $ifNull: ['$BUYERS', []]
             }
          },
          BUYERS:1,
          STATUS:{$first : '$crm_tasks.STATUS'},
          PRODUCT_TYPE:1,
          HS_CODES:1,
          "ADMIN_ID": {
            "$first": {
              "$first": "$crm_tasks.TASK_ASSIGNED_TO.id"
            }
          }, "ADMIN_ID": {
            "$first": {
              "$first": "$crm_tasks.TASK_ASSIGNED_TO.id"
            }
          },
        }
      })
      if(STATUS && STATUS.length){
        let newObj=[]
        if(areArraysOfObjectsIdentical(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
        }else if(isArraySubsetOfAnother(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(!isStringInArrayOfObjects(statusArr,element.name)){
              if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                   'STATUS' : {"$ne": null}
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                   'STATUS': element.status
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            }
        
          }
        }else{
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                  $and: [
                    {'STATUS' : {"$ne": null}},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                  $and: [
                    {'STATUS': element.status},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            
        
          }
        }
        
        
        statusFilter = {
          $or : newObj
        }
        
      }
      if(AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length){
        let newObj= []
        for(let i=0;i<=AVAILABLE_CONTACTS.length - 1 ; i++){
          const element = AVAILABLE_CONTACTS[i]
          if(element.alt === 'contact_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:false}}
              ]
            })
          }else if(element.alt === 'email_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_not'){
            newObj.push( {
              "EXTRA_DETAILS" : {$exists:false}
            })
          }
        }
        pipelinedata.push({
          $match:{
            $or : newObj
          }
        })
      }
      const overalldata = [...pipelinedata,
        {
          '$unwind': '$HS_CODES'
        }, {
          '$group': {
            '_id': {
              'exporter_country': '$EXPORTER_COUNTRY', 
              'hs_code': '$HS_CODES.HS_CODES'
            }, 
            'total_fob': {
              '$sum': '$HS_CODES.FOB_VALUE_USD'
            }
          }
        }, {
          '$sort': {
            '_id.exporter_country': 1, 
            'total_fob': -1
          }
        }, extrasearchobj, 
        {
          '$group': {
            '_id': null, 
            'top_hs_codes': {
              '$push': {
                'hs_code': '$_id.hs_code', 
                'total_fob': '$total_fob'
              }
            }
          }
        }, 
        {
          '$sort': {
            '_id': 1
          }
        }, 
        {
          '$project': {
            '_id': 1, 
            'top_hs_codes': {
              '$slice': [
                '$top_hs_codes', 10
              ]
            }
          }
        }, 
        {
          '$unwind': '$top_hs_codes'
        }, 
        {
          '$lookup': {
            'from': 'tbl_hsn_mapping', 
            'localField': 'top_hs_codes.hs_code', 
            'foreignField': 'HS_CODE', 
            'as': 'product_type'
          }
        }, 
        {
          '$project': {
            'HS_CODE': '$top_hs_codes.hs_code', 
            'total_fob': '$top_hs_codes.total_fob', 
            'PRODUCT_TYPE': {
              '$first': '$product_type.Description'
            }
          }
        }
      ]
     
      const response = await ExporterModel.aggregate(overalldata)
      resolve({
        success:true,
        message : response
      })
    }catch(e){
      console.log('error in getCommoditiesCountFunc',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}

const getCommoditiesCountFuncV2 = ({showImports, country_name,searchParam,HS_CODES,AVAILABLE_CONTACTS,TURNOVER_RANGE,CITIES,STATUS,ORGANIZATION_TYPE}) => {
  return new Promise(async (resolve,reject) => {
    try{
      const pipelinedata = []
      let searchObj = {}
      let extrasearchobj ={}
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          [showImports ? 'BUYER_NAME' : 'EXPORTER_NAME']: {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        extrasearchobj = {
          '$match': {
            '_id.hs_code': {
              '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)
            }
          }
        }
      }
      let hsObj ={}
      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item}`))}          
        }
        extrasearchobj = {
          '$match': {
            '_id.hs_code': { '$in': HS_CODES.map(item => new RegExp(`^${item}`))} 
          }
        }
      }
     
      pipelinedata.push({
        $match : {
          $and : [
            searchObj,
            {
              [showImports ? 'BUYER_COUNTRY' : 'EXPORTER_COUNTRY']: {
                $regex: new RegExp(country_name),
                $options:'i'
              }
            },
            hsObj
          ]
        }
      })
      // if(ORGANIZATION_TYPE && ORGANIZATION_TYPE.length){
      //   pipelinedata.push({
      //     $match :{
      //       EXPORTER_NAME: {
      //         $regex : ORGANIZATION_TYPE.join("|"),
      //         $options: "i"
      //       }
      //     }
      //   })
      // }
     
      if(CITIES && CITIES.length){
        pipelinedata.push({
          $match:{
            [showImports ? 'BUYER_CITY' : 'EXPORTER_CITY'] : {$in : CITIES }
          }
        })
      }
      if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
        let newObj = []
        for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
          const element = TURNOVER_RANGE[i]
          if(element.minVal && element.maxVal){
            newObj.push({
              'FOB' : {
                $lte:element.minVal,
                $lte:element.maxVal
              }
            })
          }else{
            newObj.push({
              'FOB':{
                $gte:element.maxVal
              }
            })
          }
        }
        pipelinedata.push({
          $match:{
            $or : newObj
          }
        })
      }
      pipelinedata.push({
        $sort:{
          'FOB': -1
        }
      })
      pipelinedata.push({
        $project: {
          EXPORTER_NAME: 1,
          EXPORTER_ADDRESS: 1,
          FOB: 1,
          EXPORTER_CODE: 1,
          EXPORTER_CITY: 1,
          TOTAL_BUYERS: {
            $size: {
               $ifNull: ['$BUYERS', []]
             }
          },
          BUYERS:1,
          PRODUCT_TYPE:1,
          HS_CODES:1
        }
      })
      
      const overalldata = [...pipelinedata,
        {
          '$unwind': '$HS_CODES'
        }, {
          '$group': {
            '_id': {
              [showImports ? 'buyer_country' : 'exporter_country']: showImports ? '$BUYER_COUNTRY' : '$EXPORTER_COUNTRY', 
              'hs_code': '$HS_CODES.HS_CODES'
            }, 
            'total_fob': {
              '$sum': '$HS_CODES.FOB_VALUE_USD'
            }
          }
        }, {
          '$sort': {
            [showImports ? '_id.buyer_country' : '_id.exporter_country']: 1, 
            'total_fob': -1
          }
        }, extrasearchobj, 
        {
          '$group': {
            '_id': null, 
            'top_hs_codes': {
              '$push': {
                'hs_code': '$_id.hs_code', 
                'total_fob': '$total_fob'
              }
            }
          }
        }, 
        {
          '$sort': {
            '_id': 1
          }
        }, 
        {
          '$project': {
            '_id': 1, 
            'top_hs_codes': {
              '$slice': [
                '$top_hs_codes', 10
              ]
            }
          }
        }, 
        {
          '$unwind': '$top_hs_codes'
        }, 
        {
          '$lookup': {
            'from': 'tbl_hsn_mapping', 
            'localField': 'top_hs_codes.hs_code', 
            'foreignField': 'HS_CODE', 
            'as': 'product_type'
          }
        }, 
        {
          '$project': {
            'HS_CODE': '$top_hs_codes.hs_code', 
            'total_fob': '$top_hs_codes.total_fob', 
            'PRODUCT_TYPE': {
              '$first': '$product_type.Description'
            }
          }
        }
      ]
     
      let response = null
      if(showImports){
        response = await BuyerModelV2.aggregate(overalldata)
      }
      else{
        response = await ExporterModelV2.aggregate(overalldata)
      }      
      resolve({
        success:true,
        message : response
      })
    }catch(e){
      console.log('error in getCommoditiesCountFuncV2',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}

exports.getCountriesCount = async (req,res) => {
  try{
    const result = await getCountriesCountFunc(req.body)
    res.send(result)
  }catch(e){
    console.log('error in ')
    res.send(e)
  }
}

const getCountriesCountFunc = ({country_name,searchParam,HS_CODES,AVAILABLE_CONTACTS,TURNOVER_RANGE,CITIES,STATUS,ORGANIZATION_TYPE}) => {
  return new Promise(async (resolve,reject) => {
    try{
      const pipelinedata = []
      let searchObj = {}
      let extrasearchobj ={}
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        extrasearchobj = {
          '$match': {
            '_id.hs_code': {
              '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)
            }
          }
        }
      }
      let hsObj ={}
      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
        }
      }
     
      pipelinedata.push({
        $match : {
          $and : [
            searchObj,
            {
              'EXPORTER_COUNTRY': {
                $regex: new RegExp(country_name),
                $options:'i'
              }
            },
            hsObj
          ]
        }
      })
      if(ORGANIZATION_TYPE && ORGANIZATION_TYPE.length){
        pipelinedata.push({
          $match :{
            EXPORTER_NAME: {
              $regex : ORGANIZATION_TYPE.join("|"),
              $options: "i"
            }
          }
        })
      }
     
      if(CITIES && CITIES.length){
        pipelinedata.push({
          $match:{
            'EXPORTER_CITY' : {$in : CITIES }
          }
        })
      }
      if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
        let newObj = []
        for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
          const element = TURNOVER_RANGE[i]
          if(element.minVal && element.maxVal){
            newObj.push({
              'FOB' : {
                $lte:element.minVal,
                $lte:element.maxVal
              }
            })
          }else{
            newObj.push({
              'FOB':{
                $gte:element.maxVal
              }
            })
          }
        }
        pipelinedata.push({
          $match:{
            $or : newObj
          }
        })
      }
      pipelinedata.push({
        $sort:{
          'FOB': -1
        }
      })
      pipelinedata.push({
        $lookup:{
          from: env === 'dev' ? 'tbl_crm_tasks' : 'tbl_crm_tasks_prod',
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'crm_tasks'
        }
      })
      pipelinedata.push({
        $project: {
          EXPORTER_NAME: 1,
          EXPORTER_ADDRESS: 1,
          FOB: 1,
          EXPORTER_CODE: 1,
          EXPORTER_CITY: 1,
          EXTRA_DETAILS: {$first: '$crm_tasks.EXTRA_DETAILS'},
          TASK_ASSIGNED_TO:{$first:"$crm_tasks.TASK_ASSIGNED_TO"},
          TOTAL_BUYERS: {
            $size: {
               $ifNull: ['$BUYERS', []]
             }
          },
          BUYERS:1,
          STATUS:{$first : '$crm_tasks.STATUS'},
          PRODUCT_TYPE:1,
          HS_CODES:1,
          EXPORTER_COUNTRY:1,
          EXPORTER_COUNTRY_CODE:1,
          EXPORTER_REGION:1,
          "ADMIN_ID": {
            "$first": {
              "$first": "$crm_tasks.TASK_ASSIGNED_TO.id"
            }
          },
        }
      })
      if(STATUS && STATUS.length){
        let newObj=[]
        if(areArraysOfObjectsIdentical(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
        }else if(isArraySubsetOfAnother(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(!isStringInArrayOfObjects(statusArr,element.name)){
              if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                   'STATUS' : {"$ne": null}
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                   'STATUS': element.status
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            }
        
          }
        }else{
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                  $and: [
                    {'STATUS' : {"$ne": null}},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                  $and: [
                    {'STATUS': element.status},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            
        
          }
        }
        
        
        statusFilter = {
          $or : newObj
        }
        
      }
      if(AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length){
        let newObj= []
        for(let i=0;i<=AVAILABLE_CONTACTS.length - 1 ; i++){
          const element = AVAILABLE_CONTACTS[i]
          if(element.alt === 'contact_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:false}}
              ]
            })
          }else if(element.alt === 'email_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_not'){
            newObj.push( {
              "EXTRA_DETAILS" : {$exists:false}
            })
          }
        }
        pipelinedata.push({
          $match:{
            $or : newObj
          }
        })
      }
      const overalldata = [...pipelinedata,
        {
          '$group': {
            '_id': '$EXPORTER_COUNTRY', 
            'total_exporters': {
              '$sum': 1
            }, 
            'region_name': {
              '$first': '$EXPORTER_REGION'
            }, 
            'sortname': {
              '$first': '$EXPORTER_COUNTRY_CODE'
            }, 
            'country': {
              '$first': '$EXPORTER_COUNTRY'
            },
          }
        }
      ]
     
      const response = await ExporterModel.aggregate(overalldata)
      resolve({
        success:true,
        message : response
      })
    }catch(e){
      console.log('error in e',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}

exports.getCountriesCountV2 = async (req,res) => {
  try{
    const result = req.body.groupParam === 'Commodities' ? await getCommoditiesCountFunc(req.body) : await getCountriesCountFuncV2(req.body)
    res.send(result)
  }catch(e){
    console.log('error in ')
    res.send(e)
  }
}

const getCountriesCountFuncV2 = ({searchParam,HS_CODES,TURNOVER_RANGE,groupParam,BUYERS,COUNTRIES,country_name}) => {
  return new Promise(async (resolve,reject) => {
    try{
      const pipelinedata = []
      const reddisVariable = `${groupParam}-${country_name}-${searchParam}-${HS_CODES?.join(",")}-${TURNOVER_RANGE?.map(item => item.max + item.min).join(",") || ''}-${BUYERS?.join(",") || ''}-${COUNTRIES?.join(",") || ""}`
      const cachedData = await redisInstance.redisGetSync(reddisVariable)
      if(cachedData){
        return resolve({
          success:true,
          message:JSON.parse(cachedData)
        })
      }
       const matchConditions = [
        searchParam ? {
          $or: [
            { 'EXPORTER_NAME': { $regex: new RegExp(`${searchParam}`, 'i') } },
            { "HS_CODE": { $regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`) } }
          ]
        } : {},
        country_name ? { 'EXPORTER_COUNTRY': country_name } : {},
        BUYERS && BUYERS.length ? { 'CONSIGNEE_CODE': { $in: BUYERS } } : {},
        COUNTRIES && COUNTRIES.length ? { 'DESTINATION_COUNTRY': { $in: COUNTRIES } } : {},
        HS_CODES && HS_CODES.length ?  { 
          "HS_CODE": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
        } : {}
      ];
    
      const matchStage = {
        $match:{
          $and :matchConditions
        }
      };
      pipelinedata.push(matchStage)
      const overalldata = [...pipelinedata,
          {
            '$group': {
              '_id': groupParam ? `$${groupParam}` : '$DESTINATION_COUNTRY', 
              'total_fob': {
                '$sum': '$FOB_VALUE_USD'
              }, 
              'exporters': {
                '$addToSet': '$EXPORTER_CODE'
              },
              'buyers':{
                '$addToSet': '$CONSIGNEE_CODE'
              }
            }
          }, {
            '$project': {
              "_id": "$_id",
              "country": "$_id",
              "total_fob":1,
              "total_buyers": {
                "$size": "$buyers"
              },
              "total_exporters": {
                  "$size": "$exporters"
              }
            }
          }
      ]
      if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
        let newObj = []
        for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
          const element = TURNOVER_RANGE[i]
          if(element.minVal !== undefined && element.maxVal !== undefined){
            newObj.push({
              'total_fob' : {
                $lte:element.minVal,
                $lte:element.maxVal
              }
            })
          }else{
            newObj.push({
              'total_fob':{
                $gte:element.maxVal
              }
            })
          }
        }
        overalldata.push({
          $match:{
            $or : newObj
          }
        })
      }
      overalldata.push({
        $sort:{
          'total_fob': -1
        }
      })
      overalldata.push({
      '$limit': 10
      })


      const response = await TTVSummary.aggregate(overalldata)
      //console.log('REsponseee',JSON.stringify(overalldata));
      await redisInstance.redisSetSync(reddisVariable,JSON.stringify(response))
      resolve({
        success:true,
        message : response
      })
    }catch(e){
      console.log('error in e',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}

exports.getCountriesCountV3 = async (req,res) => {
  try{
    const result = req.body.groupParam === 'Commodities' ? await getCommoditiesCountFuncV2(req.body) : await getCountriesCountFuncV3(req.body)
    res.send(result)
  }catch(e){
    console.log('error in ')
    res.send(e)
  }
}

const getCountriesCountFuncV3 = ({showImports,searchParam,HS_CODES,TURNOVER_RANGE,groupParam,BUYERS,COUNTRIES,country_name,CITIES,ORGANIZATION_TYPE,AVAILABLE_CONTACTS,STATUS,EXPORTER_CODES,search,EXPORTER_NAMES}) => {
  return new Promise(async (resolve,reject) => {
    try{
      const pipelinedata = []
      const reddisVariable = `${showImports}-${groupParam}-${country_name}-${searchParam}-${HS_CODES.join(",")}-${TURNOVER_RANGE?.map(item => item.max + item.min).join(",") || ''}-${BUYERS?.join(",") || ''}-${COUNTRIES?.join(",") || ""}-${CITIES?.join(",") || ""}-${ORGANIZATION_TYPE?.join(",") || ""}-${AVAILABLE_CONTACTS?.map(item=> item.name)?.join(",") || ""}-${STATUS?.map(item=> item.name)?.join(",") || ""}-${EXPORTER_NAMES?.join(",") || ""}-${search || ""}`
      const cachedData = await redisInstance.redisGetSync(reddisVariable)
      let FOB_BY_HS = null
      if(cachedData){
        return resolve({
          success:true,
          message:JSON.parse(cachedData)
        })
      }
      if (HS_CODES && HS_CODES.length) {
        const hsCodesRegex = HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
        pipelinedata.push({
          $match: {
            "HS_CODES.HS_CODES": { $in: hsCodesRegex }
          }
        });
        FOB_BY_HS = {
          $sum: {
            $map: {
              input: {
                $filter: {
                  input: "$HS_CODES",
                  as: "code",
                  cond: {
                    $in: [
                      { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                      HS_CODES
                    ]
                  }
                }
              },
              as: "code",
              in: "$$code.FOB_VALUE_USD"
            }
          }
        } 
      }
      if(searchParam){
        if(!isNaN(parseInt(searchParam))){
          FOB_BY_HS = {
            $sum: {
              $map: {
                input: {
                  $filter: {
                    input: "$HS_CODES",
                    as: "code",
                    cond: {
                      $regexMatch: {
                        input: "$$code.HS_CODES",
                        regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)
                      }
                    }
                  }
                },
                as: "code",
                in: "$$code.FOB_VALUE_USD"
              }
            }
          }
        }
      }
      let  organiztionType = {}
      if(ORGANIZATION_TYPE && ORGANIZATION_TYPE.length >= 1){
        let newObj = []
        if(ORGANIZATION_TYPE.includes("Others") && ORGANIZATION_TYPE.length > 1){
          newObj.push({
            'EXPORTER_NAME':{
              $regex: new RegExp(ORGANIZATION_TYPE.filter(item => item !== 'Others').join("|")), $options:'i'
            }
          })
          newObj.push({
            'EXPORTER_NAME': { $not: {$regex:/PVT LTD|PUB LTD|LLP/,$options:'i'}}
          })
        }else if(ORGANIZATION_TYPE.includes("Others")){
          newObj.push({
            'EXPORTER_NAME': {
              $not: /pvt|pub|llp/i
            }
          })
        }else{
          newObj.push({
            'EXPORTER_NAME': {
              $regex:new RegExp(ORGANIZATION_TYPE.filter(item => item !== 'Others').join("|"),'i') , 
            }
          })
        }
        
        organiztionType = {
          $or : newObj
        }
      }
  
      let projectStage = {
        $project : {
          EXPORTER_NAME: showImports ? '$BUYER_NAME' : '$EXPORTER_NAME',
          EXPORTER_ADDRESS: showImports ? '$BUYER_ADDRESS' : '$EXPORTER_ADDRESS',
          FOB: 1,
          EXPORTER_CODE: showImports ? '$BUYER_CODE' : '$EXPORTER_CODE',
          EXPORTER_CITY: showImports ? '$BUYER_CITY' : '$EXPORTER_CITY',
          EXTRA_DETAILS: 1,
          TASK_ASSIGNED_TO:1,
          TOTAL_BUYERS: {
            $size: {
               $ifNull: [showImports ? '$EXPORTERS' : '$BUYERS', []]
             }
          },
          BUYERS:showImports ? '$EXPORTERS' : '$BUYERS',
          STATUS:1,
          HS_CODES:1,
          CIN_NO:1,
          AUDITOR_DATA:1,
          'ADMIN_ID':{
            $first : '$TASK_ASSIGNED_TO.id'
          },
          EXPORT_COUNTRIES: showImports ? '$IMPORT_COUNTRIES' : '$EXPORT_COUNTRIES',
          EXPORTER_COUNTRY :showImports ? '$BUYER_COUNTRY' : '$EXPORTER_COUNTRY',

        }
      }
      if(FOB_BY_HS){
        projectStage["$project"]["FOB_BY_HS"] = FOB_BY_HS
      }
      pipelinedata.push(projectStage)
      let contactFilter = {}
      if(AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length){
        let newObj= []
        for(let i=0;i<=AVAILABLE_CONTACTS.length - 1 ; i++){
          const element = AVAILABLE_CONTACTS[i]
          if(element.alt === 'contact_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:false}}
              ]
            })
          }else if(element.alt === 'email_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_count'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
                {"EXTRA_DETAILS.Email ID" : {$exists:true}}
              ]
            })
          }else if(element.alt === 'both_not'){
            newObj.push( {
              $and : [
                {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
                {"EXTRA_DETAILS.Email ID" : {$exists:false}}
              ]
            })
          }
        }
        contactFilter = {
          $or : newObj
        }
      }
      let  turnoverFilter = {}
      if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
        let newObj = []
        for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
          const element = TURNOVER_RANGE[i]
          if(element.minVal !== undefined && element.maxVal !== undefined){
            newObj.push({
              [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB'] : {
                $gte:element.minVal,
                $lte:element.maxVal
              }
            })
          }else{
            newObj.push({
              [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB']:{
                $gte:element.maxVal
              }
            })
          }
        }
        turnoverFilter = {
          $or : newObj
        }
      }
      let statusFilter = {}
      if(STATUS && STATUS.length){
        let newObj=[]
        if(areArraysOfObjectsIdentical(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
        }else if(isArraySubsetOfAnother(statusArr,STATUS,"name")){
          newObj.push(
            {"$or": [ {
              "ADMIN_ID": {
                "$ne": null
              }
            }]}
          )
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(!isStringInArrayOfObjects(statusArr,element.name)){
              if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                   'STATUS' : {"$ne": null}
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                   'STATUS': element.status
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            }
        
          }
        }else{
          for(let i = 0; i<= STATUS.length - 1;i++){
            const element = STATUS[i]
            if(element.status != undefined || element.status != null){
             
                if(element.status === 0){
                 newObj.push({
                  $and: [
                    {'STATUS' : {"$ne": null}},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                 })
                }else if(element.status === 'Pending'){
                 newObj.push({
                   $and: [
                     {'STATUS' : 0},
                     {
                       "$or": [ {
                         "ADMIN_ID": {
                           "$ne": null
                         }
                       }]
                     }
                   ]
                 })
                }
                else{
                 newObj.push({
                  $and: [
                    {'STATUS': element.status},
                    {
                      "$or": [ {
                        "ADMIN_ID": {
                          "$ne": null
                        }
                      }]
                    }
                  ]
                   
                  })
                }
               }else if(element.name === 'Not assigned'){
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$eq": null
                     }
                   }]} 
                 )
               }else{
                 newObj.push(
                   {"$or": [ {
                     "ADMIN_ID": {
                       "$ne": null
                     }
                   }]}
                 )
               }
            
        
          }
        }
        
        
        statusFilter = {
          $or : newObj
        }
        
      }
  
      const matchConditions = [
        searchParam ? {
          $or: [
            { ['EXPORTER_NAME']: { $regex: new RegExp(`${searchParam}`, 'i') } },
            { "HS_CODES.HS_CODES": { $regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`) } }
          ]
        } : {},
        country_name ? { ['EXPORTER_COUNTRY']: country_name } : {},
        EXPORTER_NAMES && EXPORTER_NAMES.length ? {['EXPORTER_NAME']: {$in: EXPORTER_NAMES}} :{},
        search ? { 
          $or: [
            {['EXPORTER_NAME']: {$regex: new RegExp(search) , $options:'i'}},
            { 'EXTRA_DETAILS.Contact Number': {$regex: new RegExp(search),$options:'i'}}
          ] 
        } : {},
        BUYERS && BUYERS.length ? { ['BUYERS']: { $in: BUYERS } } : {},
        COUNTRIES && COUNTRIES.length ? { ['EXPORT_COUNTRIES']: { $in: COUNTRIES } } : {},
        ORGANIZATION_TYPE && ORGANIZATION_TYPE.length ? organiztionType : {},
        CITIES && CITIES.length ? { ['EXPORTER_CITY']: { $in: CITIES } } : {},
        TURNOVER_RANGE && TURNOVER_RANGE.length ? turnoverFilter : {},
        STATUS && STATUS.length ? statusFilter : {},
        AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length? contactFilter : {}
      ];

    
      const matchStage = {
        $match:{
          $and :matchConditions
        }
      };
      // console.log("matchConditionsssssssssssssssssssssssssss", matchConditions);
      pipelinedata.push(matchStage)
    
      
      const response = showImports ? await BuyerModelV2.aggregate(pipelinedata) : await ExporterModelV2.aggregate(pipelinedata)
      let hsComp = {}
      if(HS_CODES && HS_CODES.length){
        const hsCodesRegex = HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
        hsComp = {
          'HS_CODE': {
            $in : hsCodesRegex
          }
        }
      }

      
      const overalldata = [
        {
          $match : {
            [showImports ? 'CONSIGNEE_NAME' : 'EXPORTER_NAME'] : {
              $in : response.map(item => item.EXPORTER_NAME)
            },
            ...hsComp           
          }
        },
          {
            '$group': {
              '_id': groupParam === 'DESTINATION_COUNTRY' ? showImports ? '$ORIGIN_COUNTRY' : '$DESTINATION_COUNTRY' : `$${groupParam}` , 
              'total_fob': {
                '$sum': '$FOB_VALUE_USD'
              }, 
              [showImports ? 'buyers' : 'exporters']: {
                '$addToSet': showImports ? '$CONSIGNEE_NAME' : '$EXPORTER_NAME'
              },
              [showImports ? 'exporters' : 'buyers']:{
                '$addToSet': showImports ? '$EXPORTER_NAME' : '$CONSIGNEE_NAME'
              }
            }
          }, {
            '$project': {
              "_id": "$_id",
              "country": "$_id",
              "total_fob":1,
              "total_buyers": {
                "$size": "$buyers"
              },
              "total_exporters": {
                  "$size": "$exporters"
              }
            }
          }
      ]   
      if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
        let newObj = []
        for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
          const element = TURNOVER_RANGE[i]
          if(element.minVal !== undefined && element.maxVal !== undefined){
            newObj.push({
              'total_fob' : {
                $lte:element.minVal,
                $lte:element.maxVal
              }
            })
          }else{
            newObj.push({
              'total_fob':{
                $gte:element.maxVal
              }
            })
          }
        }
        overalldata.push({
          $match:{
            $or : newObj
          }
        })
      }
      overalldata.push({
        $sort:{
          'total_fob': -1
        }
      })
      overalldata.push({
      '$limit': 10
      })
      const responsefinal =showImports ? await TTVModelV2.aggregate(overalldata) : await TTV.aggregate(overalldata)
      await redisInstance.redisSetSync(reddisVariable,JSON.stringify(responsefinal))
      resolve({
        success:true,
        message : responsefinal
      })
    }catch(e){
      console.log('error in e',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}


exports.getCRMMasterTblFilters = async (req,res) => {
  try{
    const result = await getCRMMasterTblFiltersFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCRMMasterTblFiltersFunc = async ({country_name,searchParam,HS_CODES,onlyShowForUserId,BUYERS,COUNTRIES,EXPORTER_CODES}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let pipelinedata = []
      let searchObj = {}
      const reddisVariable = `${country_name}-${searchParam}-${HS_CODES.join(",")}-${onlyShowForUserId ? onlyShowForUserId : ''}-${BUYERS?.join(",") || ''}-${COUNTRIES?.join(",") || ""}`
      const cachedData = await redisInstance.redisGetSync(reddisVariable)
      if(cachedData){
        return resolve({
          success:true,
          message:JSON.parse(cachedData)
        })
      }
      const matchConditions = [
        searchParam ? {
          $or: [
            { 'EXPORTER_NAME': { $regex: new RegExp(`${searchParam}`, 'i') } },
            { "HS_CODES.HS_CODES": { $regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`) } }
          ]
        } : {},
        EXPORTER_CODES ? {'EXPORTER_CODE': {$in: EXPORTER_CODES}} :{},
        country_name ? { 'EXPORTER_COUNTRY': country_name } : {},
        BUYERS && BUYERS.length ? { 'BUYERS': { $in: BUYERS } } : {},
        COUNTRIES && COUNTRIES.length ? { 'EXPORT_COUNTRIES': { $in: COUNTRIES } } : {},
        HS_CODES && HS_CODES.length ? { 'HS_CODES.HS_CODES': {$in:  HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))}} : {}
      ];
    
      const matchStage = {
        $match: {
          $and : matchConditions
        }
      };  
      pipelinedata = [matchStage]

      pipelinedata.push({
        $lookup:{
          from: env === 'dev' ? 'tbl_crm_tasks' : 'tbl_crm_tasks_prod',
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'crm_tasks'
        }
      })
      pipelinedata.push({
        $project: {
          'EXPORTER_NAME': 1,
          'EXPORTER_CITY': 1,
          EXTRA_DETAILS: {$first: '$crm_tasks.EXTRA_DETAILS'}        }
      })

      const otherpipeline = [...pipelinedata]
      otherpipeline.push({
        $unwind : "$EXTRA_DETAILS"
      })
      otherpipeline.push( {
        $group: {
          _id: null,
          'Contact_Person': {
            $addToSet : {
              name:'$EXTRA_DETAILS.Contact Person'
            }
          },
          'Contact_Number': {
            $addToSet : {
              name:'$EXTRA_DETAILS.Contact Number'
            }
          },
           'Designation': {
            $addToSet : {
              name:'$EXTRA_DETAILS.Designation'
            }
          }
        }
      })
     
     
      pipelinedata.push({
        $group : {
          '_id': null,
          'EXPORTER_CITY': {
            '$addToSet': {
              name:'$EXPORTER_CITY'
            }
          },
          'EXPORTER_NAME':{
            '$addToSet': {
              'name' : '$EXPORTER_NAME'
            }
          }
        }
      })
      const p1 = ExporterModel.aggregate(pipelinedata)
      const p2 = ExporterModel.aggregate(otherpipeline)
      const [response,response2] = await Promise.all([p1,p2])
  
      let filterData = {}
      filterData["Company Name"] = {
        "accordianId": 'companyName',
        type: "checkbox",
        labelName: "name",
        data : response?.[0]?.EXPORTER_NAME
      }

  
      filterData["Contact No"] = {
        "accordianId": 'contactNo',
        type: "checkbox",
        labelName: "name",
        data: response2?.[0]?.Contact_Number
      }

      filterData["Contact Person"] = {
        "accordianId": 'contactPerson',
        type: "checkbox",
        labelName: "name",
        data: response2?.[0]?.Contact_Person
      }
  
      filterData["Designation"] = {
        "accordianId": 'designation',
        type: "checkbox",
        labelName: "name",
        data: response2?.[0]?.Designation
      }

      filterData["Exporter City"] =  {
        "accordianId": 'CITIES',
        type: "checkbox",
        labelName: "name",
        data:response?.[0]?.EXPORTER_CITY
      }
      if(!onlyShowForUserId){
        filterData["Lead Assigned To"] = {
          "accordianId": 'leadAssignedTo',
          type: "checkbox",
          labelName: "name"
        }
        let query = `SELECT tbl_user_details.contact_person AS name FROM tbl_user 
        LEFT JOIN tbl_user_details ON tbl_user.id = tbl_user_details.tbl_user_id WHERE tbl_user.isSubUser = 1 AND tbl_user.type_id = 1 `
        let dbRes = await call({ query }, 'makeQuery', 'get');
        filterData["Lead Assigned To"]["data"] = dbRes.message
      }
      await redisInstance.redisSetSync(reddisVariable,JSON.stringify(filterData))
      resolve({
        success:true,
        message : filterData
      })
    }catch(e){
      console.log('error in e',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}


exports.addExtraContactDetails = async (req,res) => {
  try{
    const {EXPORTER_CODE,contactObject,isUpdate} = req.body
    let response 
    if(isUpdate){
      response = await ExporterModelV2.updateOne({EXPORTER_CODE:EXPORTER_CODE,"EXTRA_DETAILS._id":contactObject._id},{$set:{"EXTRA_DETAILS.$":contactObject}})
      console.log('responsee',response);
      return res.send({
        success:true,
        message: 'Contact details updated succesfully'
      })
    }else{
      response = await ExporterModelV2.updateOne({EXPORTER_CODE:EXPORTER_CODE},{$push:{"EXTRA_DETAILS":contactObject }})
      if(response.modifiedCount === 0){
        const exporterlist = await ExporterModel.find({EXPORTER_CODE:EXPORTER_CODE})
        if(exporterlist.length >= 1){
          const exporterObj = exporterlist[0]
          const crmObj = {
            EXPORTER_CODE:exporterObj.EXPORTER_CODE,
            EXPORTER_NAME:exporterObj.EXPORTER_NAME,
            EXPORTER_ADDRESS:exporterObj.EXPORTER_ADDRESS,
            EXPORTER_CITY: exporterObj.EXPORTER_CITY,
            TOTAL_BUYERS:exporterObj.BUYERS.length,
            TOTAL_SHIPMENTS:exporterObj.TOTAL_SHIPMENTS,
            FOB: exporterObj.FOB,
            HS_CODES:exporterObj.HS_CODES,
            EXTRA_DETAILS:[contactObject],
            STATUS:0,
          }
          const TOP_COUNTRIES = await TTV.aggregate([
            {
              '$match': {
                'EXPORTER_CODE': new RegExp(`^${exporterObj.EXPORTER_CODE}`)
              }
            }, {
              '$group': {
                '_id': '$DESTINATION_COUNTRY', 
                'total_shipments': {
                  '$sum': 1
                }, 
                'FOB': {
                  '$sum': '$FOB_VALUE_USD'
                }, 
                'destination_country': {
                  '$first': '$DESTINATION_COUNTRY'
                }
              }
            }, {
              '$sort': {
                'FOB': -1
              }
            }, {
              '$limit': 2
            }
          ])
          crmObj["TOP_COUNTRIES"] = TOP_COUNTRIES
          response = await ExporterModelV2.create(crmObj)
          return res.send({
            success:true,
            message: 'Contact details added succesfully'
          })
      }
      }
    
      if(response.modifiedCount){
        return res.send({
          success:true,
          message: 'Contact details added succesfully'
        })
      }else{
        console.log('error in e',e);
          return res.send({
          success:false,
          message: 'Failed to update contact details'
        })
      }
    }
  }catch(e){
    return res.send({
      success:false,
      message: e
    })
  }
}

exports.getExporterByTTVCode =async (req,res) => {
  try{
    const exporterdetails = await ExporterModelV2.find({EXPORTER_CODE:req.body.ttvExporterCode})
    if (exporterdetails.length) {
      let tbl_user_id = exporterdetails?.[0]?.tbl_user_id
      const query = ` SELECT 
      tbl_user.* ,
      subAdmin.contact_person as TaskAssignedToName,
      tbl_user_details.company_country,
      tbl_user_details.company_name,
      tbl_user_details.contact_number,
      tbl_user_details.contact_person,
      tbl_user_details.country_code,
      tbl_user_details.email_id
      
      FROM tbl_user

    LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
    LEFT JOIN tbl_user_details subAdmin ON subAdmin.tbl_user_id = tbl_user.LeadAssignedTo

    WHERE tbl_user.id = '${tbl_user_id}'
  `
      const dbRes = await call({ query }, 'makeQuery', 'get')
      res.send({
        success: true,
        message: dbRes.message?.[0] || {}
      })
    }else{
      res.send({
        success: false,
        message: 'User Id Not Found'
      })
    }

  }catch(e){
    console.log('Error in eeee',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.getHSTrendGraph = async(req,res) => {
  try{
    const result = await getHSTrendGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getHSTrendGraphFunc = ({HSTrendGraphDuration,priceHistoryFrom,priceHistoryTo,EXPORTER_CODE,selectedHS,searchParam,HS_CODES,EXPORTER_CODES}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths 
      if(HSTrendGraphDuration){
        countForMonths = HSTrendGraphDuration?.split(" ")[0] / 1
      }else{
        countForMonths = moment(priceHistoryTo).diff(priceHistoryFrom,'month')
      }
      let dateFormat = ''
      if(countForMonths > 12){
        dateFormat = '%Y'
      }else if(countForMonths > 3){
        dateFormat = '%Y-%m-01'
      }else if(countForMonths === 1){
        dateFormat = '%Y-%m-%d'
      }else{
        dateFormat = "W%V"
      }
      let toppipeline = []
      let searchObj = {}
      let hs_len = {
        $substr: ["$HS_CODE",0,6]
      }
      let hsnt5len =  {
        $substr: ['$HS_CODES.HS_CODES',0,6]
      }
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        toppipeline.push({
          $match : {
            $and : [
              searchObj,
              EXPORTER_CODES ? {'EXPORTER_CODE': {$in: EXPORTER_CODES}} :{},
            ]
          }
        })
        hs_len = isNaN(parseInt(searchParam)) ? {
          $substr: ["$HS_CODE",0,2]
        } : {
          $substr: ["$HS_CODE",0,6]
        }
        hsnt5len = isNaN(parseInt(searchParam)) ? {
          $substr:['$HS_CODES.HS_CODES',0,2]
        } : {
          $substr: ['$HS_CODES.HS_CODES',0,6]
        }
      }
      if(EXPORTER_CODE){
        toppipeline.push({
          '$match': {
            'EXPORTER_CODE': EXPORTER_CODE
          }
        })
      }
      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
        }
      }

      if(selectedHS){
          toppipeline.push({
            '$match': {
              'HS_CODES.HS_CODES': {
                $regex : new RegExp("^" + selectedHS) 
              }
            }
          })
          hs_len = {
            $substr: ["$HS_CODE",0,6]
          }
          hsnt5len =  {
            $substr: ['$HS_CODES.HS_CODES',0,6]
          }
      }
      toppipeline.push({
        '$unwind': {
          'path': '$HS_CODES', 
          'includeArrayIndex': 'data', 
          'preserveNullAndEmptyArrays': true
        }
      })
      if(selectedHS){
        toppipeline.push({
          '$match': {
            'HS_CODES.HS_CODES': {
              $regex : new RegExp("^" + selectedHS) 
            }
          }
        })
      }
      if(HS_CODES && HS_CODES.length){
        toppipeline.push({
          '$match': {
            "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
          }
        })
      }
      toppipeline = [...toppipeline,{
        '$project': {
          'HS_CODES': 1
        }
      }, {
        '$group': {
          '_id': hsnt5len, 
          'FOB': {
            '$sum': '$HS_CODES.FOB_VALUE_USD'
          }
        }
      }, {
        '$sort': {
          'FOB': -1
        }
      }, {
        '$limit': 5
      }, {
        '$project': {
          '_id': 0, 
          'HS_CODES': '$_id', 
          'FOB': '$FOB'
        }
      }]
      const top5HS = await ExporterModel.aggregate(toppipeline)

      let pipelinedata = []
      let mainSearchObj = {}
      if(searchParam){
        mainSearchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        pipelinedata.push({
          $match: {
            $and : [
              mainSearchObj,
              EXPORTER_CODES ? {'EXPORTER_CODE': {$in: EXPORTER_CODES}} :{},
            ]
          }        
        })
      }
      if(priceHistoryFrom && priceHistoryTo){
        pipelinedata.push({ 
          $match : {
           'DATE' :{
            $gte: new Date(priceHistoryFrom),
            $lte: new Date(priceHistoryTo)
           }
          }
        })
      }
      pipelinedata = [ ...pipelinedata,
        {
          $match: {
            'HS_CODE': {
              $in : top5HS.map(item => new RegExp(`^${item.HS_CODES}`))
            }
          }
        },
        {
          $group: {
            '_id': {
                DATE_STRING: {
                $dateToString: { format: dateFormat, date: "$DATE" }
              },
              HS_CODE:hs_len
            },
            AVG_PRICE_USD: {$avg: '$STANDARD_UNIT_PRICE_USD'}
          }
        },
        {
          $group: {
            _id: '$_id.DATE_STRING',
            'HS_CODES': {
                '$push': {
                  'HS_CODE': '$_id.HS_CODE',
                  'AVG_PRICE_USD': '$AVG_PRICE_USD'
                }
            }
          } 
        },{
          $project : {
            _id:0,
            HS_CODES:1,
            label:"$_id"
          }
        },
        {
          $sort: {
            'label':1
          }
        }
      ]
      
      const response = await TTV.aggregate(pipelinedata)

      let finaldata =[]
      response.forEach(item => {
        let finalobj ={}
        finalobj.label = item.label
        
        item.HS_CODES.forEach(element => {
            finalobj[`${element.HS_CODE}`] = parseFloat(element.AVG_PRICE_USD?.toFixed(2)) 
        }) 
        finaldata.push(finalobj)
      })
      let chartconfig= []
      top5HS.forEach((element,index) => {
        chartconfig.push({
          dataKey:`${element.HS_CODES}`,
          fill:hsncolors[index],
          display:element.HS_CODES
        })
      })
      resolve({
        success:true,
        message:{
          message:finaldata,
          chartconfig,
        }
      })
    }catch(e){
      console.log('error in HScgraph',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getHSNListCRM = async (req,res) => {
  try{
    const result = await getHSNListCRMFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getHSNListCRMFunc = async ({ttvExporterCode,resultPerPage,currentPage,search}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let searchObj = {}
      if(search){
        searchObj = {
          'HS_CODE': {$regex: new RegExp("^"+search), $options:'i'}
        }
      }
      const mainpipeline = [
        {
          '$match': {
            'EXPORTER_CODE': ttvExporterCode
          }
        }, {
          '$group': {
            '_id': {
              'HS_CODE': {
                '$substr': [
                  '$HS_CODE', 0, 2
                ]
              }, 
              'DESTINATION_COUNTRY': '$DESTINATION_COUNTRY'
            }, 
            'TOTAL_SHIPMENTS': {
              '$sum': 1
            }, 
            'SUB_CODES': {
              '$addToSet': '$HS_CODE'
            }, 
            'BUYERS': {
              '$addToSet': '$CONSIGNEE_CODE'
            }, 
            'FOB': {
              '$sum': '$FOB_VALUE_USD'
            }, 
            'HS_TWO_DIGIT': {
              '$first': {
                '$substr': [
                  '$HS_CODE', 0, 2
                ]
              }
            }
          }
        }, {
          '$sort': {
            'FOB': -1
          }
        }, {
          '$group': {
            '_id': '$_id.HS_CODE', 
            'TOTAL_SHIPMENTS': {
              '$sum': '$TOTAL_SHIPMENTS'
            }, 
            'FOB': {
              '$sum': '$FOB'
            }, 
            'TOP_COUNTRIES': {
              '$push': {
                'DESTINATION_COUNTRY': '$_id.DESTINATION_COUNTRY', 
                'FOB_BY_COUNTRY': '$FOB'
              }
            }, 
            'BUYERS': {
              '$addToSet': '$BUYERS'
            }, 
            'SUB_CODES': {
              '$addToSet': '$SUB_CODES'
            }
          }
        }, {
          '$project': {
            '_id': 1, 
            'TOTAL_SHIPMENTS': 1, 
            'FOB': 1, 
            'TOP_COUNTRIES': 1, 
            'BUYERS': {
              '$reduce': {
                'input': '$BUYERS', 
                'initialValue': [], 
                'in': {
                  '$setUnion': [
                    '$$value', '$$this'
                  ]
                }
              }
            }, 
            'SUB_CODES': {
              '$reduce': {
                'input': '$SUB_CODES', 
                'initialValue': [], 
                'in': {
                  '$setUnion': [
                    '$$value', '$$this'
                  ]
                }
              }
            }
          }
        }, {
          '$lookup': {
            'from': 'tbl_hsn_mapping', 
            'localField': '_id', 
            'foreignField': 'HS_CODE', 
            'as': 'hsn_master'
          }
        }, {
          '$project': {
            '_id': 0, 
            'HS_CODE': '$_id', 
            'PRODUCT_DESCRIPTION': {
              '$first': '$hsn_master.Description'
            }, 
            'TOTAL_SHIPMENTS': 1, 
            'SUB_CODES': {
              '$size': '$SUB_CODES'
            }, 
            'BUYERS': {
              '$size': '$BUYERS'
            }, 
            'FOB': 1, 
            'TOP_COUNTRIES': {
              '$slice': [
                '$TOP_COUNTRIES', 3
              ]
            }
          }
        },
        {
          $sort: {
            'FOB':-1
          }
        }
      ]
      const countPipeline = [...mainpipeline]
      countPipeline.push({
        $count:'total_records'
      })
      
      if(currentPage && resultPerPage) {
        mainpipeline.push({
          '$skip': (currentPage - 1) * parseInt(resultPerPage) 
        })
        mainpipeline.push({
          '$limit': parseInt(resultPerPage) 
        })
      }
      const response = await TTV.aggregate(mainpipeline)
      const countRes = await TTV.aggregate(countPipeline)
      resolve({
        success:true,
        message:{
          message:response,
          total_records: countRes?.[0]?.total_records || 0
        }
      })
    }catch(e){
      console.log('error in buyers API',e);
      reject({
        success:false,
        message:''
      })
    }
  })
  
}

exports.getHSExportTrendGraph = async(req,res) => {
  try{
    const result = await getHSExportTrendGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getHSExportTrendGraphFunc = ({HSTrendGraphDuration,priceHistoryFrom,priceHistoryTo,EXPORTER_CODE,selectedHS,searchParam,HS_CODES,EXPORTER_CODES}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths 
      if(HSTrendGraphDuration){
        countForMonths = HSTrendGraphDuration?.split(" ")[0] / 1
      }else{
        countForMonths = moment(priceHistoryTo).diff(priceHistoryFrom,'month')
      }
      let dateFormat = ''
      if(countForMonths > 12){
        dateFormat = '%Y'
      }else if(countForMonths > 3){
        dateFormat = '%Y-%m-01'
      }else if(countForMonths === 1){
        dateFormat = '%Y-%m-%d'
      }else{
        dateFormat = "W%V"
      }
      let toppipeline = []
      let searchObj = {}
      let hs_len = {
        $substr: ["$HS_CODE",0,6]
      }
      let hsnt5len =  {
        $substr: ['$HS_CODES.HS_CODES',0,6]
      }
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        toppipeline.push({
          $match : {
            $and : [
              searchObj,
              EXPORTER_CODES ? {'EXPORTER_CODE': {$in: EXPORTER_CODES}} :{},
            ]
          }
        })
        hs_len = isNaN(parseInt(searchParam)) ? {
          $substr: ["$HS_CODE",0,2]
        } : {
          $substr: ["$HS_CODE",0,6]
        }
        hsnt5len = isNaN(parseInt(searchParam)) ? {
          $substr:['$HS_CODES.HS_CODES',0,2]
        } : {
          $substr: ['$HS_CODES.HS_CODES',0,6]
        }
      }
      if(EXPORTER_CODE){
        toppipeline.push({
          '$match': {
            'EXPORTER_CODE': EXPORTER_CODE
          }
        })
      }
      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
        }
      }

      if(selectedHS){
          toppipeline.push({
            '$match': {
              'HS_CODES.HS_CODES': {
                $regex : new RegExp("^" + selectedHS) 
              }
            }
          })
          hs_len = {
            $substr: ["$HS_CODE",0,6]
          }
          hsnt5len =  {
            $substr: ['$HS_CODES.HS_CODES',0,6]
          }
      }
      toppipeline.push({
        '$unwind': {
          'path': '$HS_CODES', 
          'includeArrayIndex': 'data', 
          'preserveNullAndEmptyArrays': true
        }
      })
      if(selectedHS){
        toppipeline.push({
          '$match': {
            'HS_CODES.HS_CODES': {
              $regex : new RegExp("^" + selectedHS) 
            }
          }
        })
      }
      if(HS_CODES && HS_CODES.length){
        toppipeline.push({
          '$match': {
            "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
          }
        })
      }
     
      toppipeline = [...toppipeline,{
        '$project': {
          'HS_CODES': 1
        }
      }, {
        '$group': {
          '_id':hsnt5len, 
          'FOB': {
            '$sum': '$HS_CODES.FOB_VALUE_USD'
          }
        }
      }, {
        '$sort': {
          'FOB': -1
        }
      }, {
        '$limit': 5
      }, {
        '$project': {
          '_id': 0, 
          'HS_CODES': '$_id', 
          'FOB': '$FOB'
        }
      }]
      const top5HS = await ExporterModel.aggregate(toppipeline)
      let pipelinedata = []
      let mainSearchObj = {}

      if(searchParam){
        mainSearchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        pipelinedata.push({
          $match: {
            $and : [
              mainSearchObj,
              EXPORTER_CODES ? {'EXPORTER_CODE': {$in: EXPORTER_CODES}} :{},
            ]
          }
        })
      }
      if(priceHistoryFrom && priceHistoryTo){
        pipelinedata.push({ 
          $match : {
           'DATE' :{
            $gte: new Date(priceHistoryFrom),
            $lte: new Date(priceHistoryTo)
           }
          }
        })
      }
      
      pipelinedata = [ ...pipelinedata,
        {
          $match: {
            'HS_CODE': {
              $in : top5HS.map(item => new RegExp(`^${item.HS_CODES}`))
            }
          }
        },
        {
          $group: {
            '_id': {
                DATE_STRING: {
                $dateToString: { format: dateFormat, date: "$DATE" }
              },
              HS_CODE: hs_len
            },
            Quantity: {$avg: '$QUANTITY'},
            FOB: {$sum : "$FOB_VALUE_USD"}
          }
        },
        {
          $group: {
            _id: '$_id.DATE_STRING',
            'HS_CODES': {
                '$push': {
                  'HS_CODE': '$_id.HS_CODE',
                  'Quantity': '$Quantity',
                  'FOB': '$FOB'
                }
            }
          } 
        },{
          $project : {
            _id:0,
            HS_CODES:1,
            label:"$_id"
          }
        },
        {
          $sort: {
            'label':1
          }
        }
      ]
      
      const response = await TTV.aggregate(pipelinedata)

      let finaldata =[]
      response.forEach(item => {
        let finalobj ={}
        finalobj.label = item.label
        
        item.HS_CODES.forEach(element => {
            finalobj[`${element.HS_CODE}_VALUE`] = element.FOB
            finalobj[`${element.HS_CODE}_QUANTITY`] = parseFloat(element.Quantity?.toFixed(2))
        }) 
        finaldata.push(finalobj)
      })
      let chartconfig= []
      let  quantitychartconfig =[]
      top5HS.forEach((element,index) => {
        chartconfig.push({
          dataKey:`${element.HS_CODES}_VALUE`,
          fill:hsncolors[index],
          display:element.HS_CODES
        })
        quantitychartconfig.push({
          dataKey:`${element.HS_CODES}_QUANTITY`,
          fill:hsncolors[index],
          display:element.HS_CODES
        })

      })
      resolve({
        success:true,
        message:{
          message:finaldata,
          chartconfig,
          quantitychartconfig,
        }
      })
    }catch(e){
      console.log('error in HScgraph',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getCallListStats = async(req,res) => {
  try{
    const result = await getCallListStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCallListStatsFunc = ({taskUpdate,search,dateRangeFilter,onlyShowForUserId,leadAssignedTo,hscodes,leadsStatus,requirements,taskStatus,included_status,taskType}) =>{
  return new Promise(async(resolve,reject)=> {
    try{
      let matchobj  = {}
      if(dateRangeFilter && dateRangeFilter.length >=1){
        if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
          matchobj = {
            $expr: {
              $eq: [
                { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
                  dateRangeFilter?.[0]  // compare with the target date string
              ]
            }
          }
             
        }else{
          matchobj = {
            'TASK_DATE' :{
              $gte: new Date(dateRangeFilter?.[0]),
              $lte: new Date(dateRangeFilter?.[1])
             }
          }
        }
      }
      let mainPipeline = [
      { 
        $match : matchobj
      },
      {
        $match : {
          'TASK_ASSIGNED_TO' : {$exists : true},
          "TASK_TYPE": "Call List"
        }
      }
      ]
      if(onlyShowForUserId){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.id":onlyShowForUserId
          }
        })
      }
      if(leadAssignedTo && leadAssignedTo.length){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
          }
        })
      }
      const leadsPipeline = [...mainPipeline]
      const onboardPipeline  = [...mainPipeline]

      let includedTasks = []
      if(taskUpdate?.includes("User Onboarded")){
        if(taskUpdate && taskUpdate.length == 1){
          includedTasks = [4]
        }else{
          includedTasks.push(4)
        }
      }
      if(hscodes && hscodes.length){
        const hsCodesRegex = hscodes.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
        mainPipeline.push({
          $match: {
            "HS_CODES.HS_CODES": { $in: hsCodesRegex }
          }
        });
      }
      if(requirements && requirements.length){
        mainPipeline.push({
          $match: {
            'INTRESTED_SERVICES' : {$in : requirements}
          }
        })
        
      }
  
    
      if(search){
        mainPipeline.push({
          $match:{
            EXPORTER_NAME: {$regex: new RegExp(search) , $options:'i'}
          }
        })
      }
      mainPipeline.push({
        $lookup : {
          from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'task_logs'
        }
      })
      let  pendingPipeline =  mainPipeline
      pendingPipeline = [...pendingPipeline]
      if(taskType === 'Exporter Wise'){
        pendingPipeline.push({
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            },
            STATUS:1,
            TASK_ASSIGNED_TO:1,
            LAST_NOTE:1,
            LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
            EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
            TASK_DATE:1,
            EVENT_TIME:{$last: '$task_logs.EVENT_TIME'},
            EXPORTER_CODE:1
          }
        })
      }
      
      mainPipeline.push(
        taskType === 'Exporter Wise'? {
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            },
            STATUS:1,
            TASK_ASSIGNED_TO:1,
            LAST_NOTE:1,
            LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
            EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
            TASK_DATE:1,
            EVENT_TIME:{$last: '$task_logs.EVENT_TIME'},
            EXPORTER_CODE:1
          }
        } :{
          '$unwind': {
              'path': '$task_logs', 
              'includeArrayIndex': 'i', 
              'preserveNullAndEmptyArrays': true
          }
        },
      )
      if(taskType === 'Task Wise'){
        mainPipeline.push({
          $project: {
            EVENT_STATUS : '$task_logs.EVENT_STATUS',
            STATUS : 1,
            LOG_TYPE:'$task_logs.LOG_TYPE',
            TASK_DATE:1 ,
            EVENT_TIME:'$task_logs.EVENT_TIME',
            EXPORTER_CODE:1
          }
        })
      }
      if(taskStatus && taskStatus.length){
        mainPipeline.push({
          $match: {
            'EVENT_STATUS' : {
              $in : taskStatus.map(item => new RegExp(item))
            }
          }
        })
        if(taskType === 'Exporter Wise'){
          pendingPipeline.push({
            $match: {
              'EVENT_STATUS' : {
                $in : taskStatus.map(item => new RegExp(item))
              }
            }
          })
        }else{
          pendingPipeline.push({
            $match: {
              '$task_logs.EVENT_STATUS' : {
                $in : taskStatus.map(item => new RegExp(item))
              }
            }
          })
        }
      } 
      if(leadsStatus && leadsStatus.length){
        if(leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
        }
      }
      if(taskUpdate){
        let statusArray = taskUpdate.filter(element => element !== 'User Onboarded' && element !== 'Lead Created')
        if(statusArray && statusArray.length ){
          mainPipeline.push({
            $match:{
              $or : [
                {
                  'STATUS': {
                    '$in': includedTasks
                  }
                },
                {$and : [
                  {'LOG_TYPE' : 'Didnt connect'},
                  {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
                },
                {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
              ]
            }
          })
          if(taskType === 'Exporter Wise'){
            pendingPipeline.push({
              $match:{
                $or : [
                  {
                    'STATUS': {
                      '$in': includedTasks
                    }
                  },
                  {$and : [
                    {'LOG_TYPE' : 'Didnt connect'},
                    {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                  ]
                  },
                  {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
                ]
              }
            })
          }else{
            pendingPipeline.push({
              $match:{
                $or : [
                  {
                    'STATUS': {
                      '$in': includedTasks
                    }
                  },
                  {$and : [
                    {'$task_logs.LOG_TYPE' : 'Didnt connect'},
                    {'$task_logs.EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                  ]
                  },
                  {'$task_logs.LOG_TYPE': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
              }
            })
          }
        }else{
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
        }
          // mainPipeline.push({
          //   $match:{
          //     $or : [
          //       {
          //         'STATUS': {
          //           '$in': includedTasks
          //         }
          //       },
          //       statusArray && statusArray.length ? {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }} : {}
          //     ]
          //   }
          // })
        
      }else{
        if(!leadsStatus){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
        }
      }
  
      const tasksOverallPipeline =  [ ...mainPipeline,   
       taskType === 'Exporter Wise'? {
        '$group': {
          '_id': null, 
          'tasksFollowup': {
            '$sum': {
              '$cond': [
                {
                  '$in': [
                    '$LOG_TYPE', [
                      'Call back', 'Didnt connect','Create New Task'
                    ]
                  ]
                }, 1, 0
              ]
            }
          }, 
          'tasksNew': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    { '$type': '$LOG_TYPE' },
                    'missing'
                  ]
                }, 1, 0
              ]
            }
          }
        }
      } : {
        '$group': {
          '_id': null, 
          'tasksFollowup': {
            '$sum': {
              '$cond': [
                {
                  '$in': [
                    '$LOG_TYPE', [
                      'Call back', 'Didnt connect','Create New Task'
                    ]
                  ]
                }, 1, 0
              ]
            }
          }, 
          'tasksNew': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    { '$type': '$LOG_TYPE' },
                    'missing'
                  ]
                }, 1, 0
              ]
            }
          }
        }
      }
      ]
      leadsPipeline.push({
        $match:{
          STATUS:1
        }
      })
      leadsPipeline.push({
        $count : 'total_records'
      })
      onboardPipeline.push({
        $match:{
          STATUS:4
        }
      })
      onboardPipeline.push({
        $count : 'total_records'
      })
      const logTypePipeline = [...mainPipeline]
      // logTypePipeline.push({
      //   $match:{
      //     STATUS:{$in : [0,1,2,3]}
      //   }
      // })
      
      logTypePipeline.push({
        $group : {
          _id: '$LOG_TYPE',
          'total_records' : {$sum: 1},
          'LOG_TYPE':{$first:'$LOG_TYPE'}
        }
      })
      mainPipeline.push({
        $group : {
          _id: '$EVENT_STATUS',
          'total_records' : {$sum: 1},
          'EVENT_TYPE':{$first : '$EVENT_STATUS'}
        }
      })
      let tasksInComplete = 0
      let tasksCompleted = 0
      console.log('pendingPipeline',JSON.stringify(pendingPipeline));
      const eventResponse = await ExporterModelV2.aggregate(mainPipeline)
      const logsResponse = await ExporterModelV2.aggregate(logTypePipeline)
      const leadsResponse = await ExporterModelV2.aggregate(leadsPipeline)
      const onboardResponse = await ExporterModelV2.aggregate(onboardPipeline)
      const pendingResponse = await ExporterModelV2.aggregate(pendingPipeline)
      const tasksOverallResponse  = await ExporterModelV2.aggregate(tasksOverallPipeline)
      for(let i=0; i<= pendingResponse.length - 1 ; i++){
        const element = pendingResponse[i]
        if(taskType === 'Exporter Wise'){
          if(element.LOG_TYPE === undefined){
            tasksInComplete += 1
          }else{
            const TasksLogs = element.task_logs
            if(TasksLogs.LOG_TYPE === 'Lead Lost' || TasksLogs.LOG_TYPE === 'User Onboarded' || TasksLogs.LOG_TYPE === 'Not Interested' || TasksLogs.LOG_TYPE === 'Didnt connect'){
              tasksCompleted += 1
            }
            else if((new Date(TasksLogs.EVENT_TIME).getTime() <= new Date(dateRangeFilter[0]).getTime() && (new Date(TasksLogs.EVENT_TIME).getTime() >= new Date(dateRangeFilter[1]).getTime()))){
              tasksCompleted += 1
            }else {
              tasksInComplete += 1
            }
          }
        }else{
          if(element.task_logs === undefined || element?.task_logs?.length === 0){
            tasksInComplete += 1
          }else{
            for(let j = 0; j<= element.task_logs.length - 1 ; j++){
              const item = element.task_logs[j]
              if(item.LOG_TYPE === 'Lead Lost' || item.LOG_TYPE === 'User Onboarded'|| item.LOG_TYPE === 'Not Interested' || item.LOG_TYPE === 'Didnt connect'){
                tasksCompleted += 1
              }else if((new Date(item.EVENT_TIME).getTime() <= new Date(dateRangeFilter[0]).getTime()) && (new Date(item.EVENT_TIME).getTime() >= new Date(dateRangeFilter[1]).getTime())){
                  if(element.task_logs[j+1]){
                  tasksCompleted += 1
                }else{
                  tasksCompleted += 1
                }
              }else{
                tasksInComplete +=1
              }
            }
          }
          
        }
      }
      resolve({
        success:true,
        message:{
          eventResponse,
          logsResponse,
          leadsCount : leadsResponse?.[0]?.total_records,
          onboardCount :onboardResponse?.[0]?.total_records,
          pendingCount :tasksInComplete,
          completedCount:tasksCompleted,
          newTaskCount : tasksOverallResponse?.[0]?.tasksNew,
          FollowupCount : tasksOverallResponse?.[0]?.tasksFollowup,
        }
      })
    }catch(e){
      console.log('error in apio',e);
      reject({
        success:false
      })
    }
  })
}

exports.getTopBuyers = async (req,res) => {
  try{
    const result = await TTV.aggregate([
      {
        '$match': {
          'EXPORTER_NAME': req.body.EXPORTER_NAME,
          'HS_CODE' : new RegExp('^' + req.body.HS_CODE)
        }
      }, {
        '$group': {
          '_id': '$CONSIGNEE_NAME',
          'FOB': {
            '$sum': '$FOB_VALUE_USD'
          }, 
          'CONSGINEE_NAME':{
            '$first':'$CONSIGNEE_NAME'
          },
          'total_shipments': {
            '$sum': 1
          }
        }
      }, {
        '$sort': {
          'FOB': -1
        }
      }
    ])
    res.send({
      success:true,
      message:result
    })
  }catch(e){
    res.send({
      success:false,
      message:e
    })
  }
}

exports.assignDailyDefaultTasks = async (req,res) => {
  try{
    const {userId,defaultTasksCount} = req.body
    if(userId){
      const dbRes = await CRMTaskAssignment.find({tbl_user_id : userId})
      if(dbRes?.length){
        //update daily Tasks
        await CRMTaskAssignment.updateOne({tbl_user_id:userId},{$set: {defaultTasksCount: defaultTasksCount}})
      }else{
        //add New Entry
        await CRMTaskAssignment.create({
          tbl_user_id: userId,
          defaultTasksCount:defaultTasksCount
        })
      }
      res.send({
        success:true,
        message:"Daily Tasks Updated"
      })
    }else{
      res.send({
        success:false,
        message:"Provide an User Id"
      })
    }
  }catch(e){
    console.log('error in assigntask',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.getDailyDefaultTasks = async (req,res) => {
  try{
    const {userId} = req.body
    if(userId){
      const dbRes = await CRMTaskAssignment.find({tbl_user_id:userId})
      res.send({
        success:true,
        message:dbRes|| {}
      })
    }else{
      res.send({
        success:false,
        message:"Provide an User Id"
      })
    }
  }catch(e){
    console.log('error in getDailyDefaultTasks',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.InsertadditionalTasksCount = async (req,res) => {
  try{
    const {userId,additionalTasksCount} = req.body
    if(userId){
      const dbRes = await CRMTaskAssignment.find({tbl_user_id: userId})
      let additionalTasksCountDb = dbRes?.[0]?.additionalTasksCount || {}
      additionalTasksCountDb = {
        ...additionalTasksCountDb,
        ...additionalTasksCount
      }
      if(dbRes?.length){
        //update daily Tasks
        await CRMTaskAssignment.updateOne({tbl_user_id:userId},{$set:{additionalTasksCount:additionalTasksCountDb}})
      }else{
        //add New Entry
        await CRMTaskAssignment.create({
          tbl_user_id:userId,
          additionalTasksCount:additionalTasksCountDb
        })
      }
      res.send({
        success:true,
        message:"Added Additional Task Count"
      })
    }else{
      res.send({
        success:false,
        message:"Provide an User Id"
      })
    }
  }catch(e){
    console.log('error in assigntask',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.getCRMCallHistory = async(req,res) => {
  try{
    const {EXPORTER_CODE} = req.body
    if(EXPORTER_CODE){
      const response = await CRMTasksLogs.aggregate([{
        $match:{
          EXPORTER_CODE:EXPORTER_CODE
        }
      },{
        $sort:{
          CREATED_AT:-1
        }
      }])
      res.send({
        success:true,
        message:response
      })
    }else{
      res.send({
        success:false,
        message:"Provide an User Id"
      })
    }
  }catch(e){
    console.log('error in assigntask',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.deleteSubAdmin = async (req,res) => {
  try{
    const query = `UPDATE tbl_user SET 	LeadAssignedTo='${req.body.AssignTo}' WHERE 	LeadAssignedTo='${req.body.deleteUserId}'`
    await dbPool.query(query)
    const assignToQuery = `SELECT tbl_user_id as id, contact_person, contact_number,name_title,designation,email_id FROM tbl_user_details WHERE tbl_user_id = '${req.body.AssignTo}' `
    const dbRes = await call({query:assignToQuery},'makeQuery','get')
    await ExporterModelV2.updateMany({"TASK_ASSIGNED_TO.id":req.body.deleteUserId},{TASK_ASSIGNED_TO:dbRes.message?.[0]})
    const deactivateQuery = `UPDATE tbl_user SET status = 0 WHERE id = '${req.body.deleteUserId}'`
    await dbPool.query(deactivateQuery)
    await CRMTaskAssignment.deleteOne({tbl_user_id:req.body.deleteUserId})
    res.send({
      success:true,
      message:'User Deleted Succesfully'
    })
  }catch(e){
    console.log('Failed to deleteee',e);
 res.send({
      success:false,
      message:'Failed to delete user'
    })
  }
}


exports.updateUserdata = async (req,res) => {
  try{
    const {name_title,phone_code,contactPerson,contact_number,email_id,organization_type,industry_type,user_address,companyCity,companyState,country_code,companyPostal,country_of_incorporation,country_of_operation,years_of_incorporation,prevNetProfit,ExisExportTurnover,ExpecExportTurnover,currency,tbl_user_id} = req.body
    //update user details table
    const query = `UPDATE tbl_user_details SET 
                      name_title='${name_title}',
                      phone_code='${phone_code}',
                      contact_person='${contactPerson}',
                      contact_number='${contact_number}',
                      email_id='${email_id}',
                      organization_type='${organization_type}',
                      industry_type='${industry_type}',
                      user_address='${user_address}',
                      company_city='${companyCity}',
                      company_state='${companyState}',
                      country_code='${country_code}',
                      company_postal_code='${companyPostal}'
                      WHERE tbl_user_id='${tbl_user_id}'`
    const updateExtraQuery = `UPDATE tbl_user_details_extra SET
                                country_of_incorporation = '${country_of_incorporation}',
                                country_of_operation = '${country_of_operation}',
                                years_of_incorporation='${years_of_incorporation}',
                                prevNetProfit='${prevNetProfit}',
                                currency='${currency}',
                                minExisting=${ExisExportTurnover  ? parseInt(ExisExportTurnover) :  0},
                                minExpected= ${ExpecExportTurnover ? parseInt(ExpecExportTurnover) :  0}
                                WHERE tbl_user_id='${tbl_user_id}'`
    
    await dbPool.query(query)
    await dbPool.query(updateExtraQuery)
    
    res.send({
      success:true,
      message:'Details Updated Succesfully'
    })
                      

  }catch(e){
    console.log('Failed to upadate',e);
    res.send({
      success:true,
      message:'Failed to update details'})
    }
}
// exports.getCorporateExporters = async (req,res) => {
//   try{
//     const pipeline = [
//       {
//         '$match': {
//           'TASK_TYPE': 'Corporate'
//         }
//       }, {
//         '$project': {
//           '_id': 0, 
//           'EXPORTER_NAME': 1, 
//           'EXPORTER_CODE': 1
//         }
//       }
//     ]
//     const response = await ExporterModelV2.aggregate(pipeline)
//     res.send({
//       success:true,
//       message:response
//     })
//   }catch(e){
//     res.send({
//       success:true,
//       message:[]
//     })
//   }
// }

// exports.getCorporateExporters = async (req, res) => {
//   try {
//     // Access the database and collection directly
//     const collection = mongoose.connection.db.collection('india_export_exporters_list');
    
//     // Perform a query using the collection's `find` method
//     const response = await collection
//       .find({ TASK_TYPE: "Corporate" }, { projection: { _id: 0, EXPORTER_NAME: 1, EXPORTER_CODE: 1 } })
//       .toArray();

//     // Send the response
//     res.send({
//       success: true,
//       message: response
//     });
//   } catch (e) {
//     console.error("Error in getCorporateExporters:", e);
//     res.status(500).send({
//       success: false,
//       message: "Internal Server Error"
//     });
//   }
// };



exports.getCorporateExporters = async (req, res) => {
  try {
    // Determine the collection name based on the environment
    const collectionName = env === 'prod' ? 'india_export_exporters_list_prod' : 'india_export_exporters_list';

    // Access the database and collection directly
    const collection = mongoose.connection.db.collection(collectionName);

    // Perform a query using the collection's `find` method
    const response = await collection
      .find({ TASK_TYPE: "Corporate" }, { projection: { _id: 0, EXPORTER_NAME: 1, EXPORTER_CODE: 1 } })
      .toArray();

    // Send the response
    res.send({
      success: true,
      message: response
    });
  } catch (e) {
    console.error("Error in getCorporateExporters:", e);
    res.status(500).send({
      success: false,
      message: "Internal Server Error"
    });
  }
};


exports.getExporterDetailsById = async (req,res) => {
  try{

  }catch(e){
    return res.send({
      success:false,
      message:''
    })
  }
}

exports.addMonthlyGoals = async (req,res) => {
  try{
    const {userId,onboarding_goal,buyeradded_goal,invDis_goal,lcDis_goal,lc_currency,inv_currency} = req.body
    const query = `SELECT * FROM  tbl_subadmin_goals WHERE tbl_user_id='${userId}'`
    const dbRes = await call({query},'makeQuery','get')
    if(dbRes.message && dbRes.message.length){
      //update existing data
      const updateQuery = `UPDATE tbl_subadmin_goals SET 
                                onboarding_goal=${onboarding_goal},
                                buyeradded_goal=${buyeradded_goal},
                                invDis_goal=${invDis_goal},
                                lcDis_goal=${lcDis_goal},
                                lc_currency='${lc_currency}',
                                inv_currency='${inv_currency}'
                            WHERE tbl_user_id='${userId}'`
      await dbPool.query(updateQuery)
    }else{
      //insert new data
      const insertQuery = `INSERT INTO tbl_subadmin_goals 
                            (tbl_user_id,onboarding_goal,buyeradded_goal,invDis_goal,lcDis_goal,lc_currency,inv_currency)
                            VALUES (${userId},${onboarding_goal},${buyeradded_goal},${invDis_goal},${lcDis_goal},'${lc_currency}','${inv_currency}')`
      await dbPool.query(insertQuery)
    }
    res.send({
      success:true,
      message: 'Goal Set For Current Month'
    })
  }catch(e){
    console.log('error in api',e);
    res.send({
      success:false,
      message: 'Failed to set Goal For Current Month'
    })
  }
}
exports.getMonthlyGoals = async (req,res) => {
  try{
    const {userId} = req.body
    const query = `SELECT * FROM  tbl_subadmin_goals WHERE tbl_user_id='${userId}'`
    const dbRes = await call({query},'makeQuery','get')
    res.send({
      success:true,
      message:dbRes.message?.[0] || {}
    })
  }catch(e){
    res.send({
      success:false,
      message:{}
    })
  }
}
exports.getFinancersByServiceType = async (req,res) => {
  try{
    let response = req.body.type === 'LC'  ? await enabledFinanciersForLC() : await emailEnabledBanks()
    res.send({
      success:true,
      message:response
    })
  }catch(e){
    res.send({
      success:true,
      message:[]
    })
  }
}

exports.createNewApplication = async (req,res) => {
  try{
    let dbFile = []
    let docIdArray = []
    const reqFiles = req.files
    const reqBody = req.body
    if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
            let fileHash = reqFiles[item].md5
            fs.writeFileSync('./docs/' + fileHash, reqFiles[item].data);
            dbFile.push({ name: item, file_name: reqFiles[item].name, hash: fileHash })
        });

        if (dbFile.length) {
            for (let i = 0; i < dbFile.length; i++) {
                let dbReqObj = {
                    "tableName": "tbl_document_details",
                    "insertObj": {
                        doc_no: dbFile[i]["file_name"],
                        doc_name: dbFile[i]["name"].split(":")[0].split("_").join(" "),
                        file_name: dbFile[i]["file_name"],
                        gen_doc_label: dbFile[i]["file_name"],
                        file_hash: dbFile[i]["hash"],
                        valid_upto: "",
                        category: dbFile[i]["name"].split(":")[1] / 1,
                        mst_doc_id: dbFile[i]["name"].split(":")[2] / 1,
                        created_at: new Date(),
                        created_by: reqBody.userId,
                        modified_at: new Date()
                    }
                }
                let dbResObj = await call(dbReqObj, 'setData', 'post');
                if (!dbResObj.success) {
                    console.log('Error while inserting data for cif sale doc:', dbResObj.message)
                    throw errors.databaseApiError;
                }
                let tbl_doc_detail_id = dbResObj.message.dataValues.id
                docIdArray.push(tbl_doc_detail_id)
            }
        }
    }
    const {EXPORTER_NAME,EXPORTER_CODE,APPLICATION_NUMBER,APPLICATION_TYPE,APPLICATION_STATUS,EXTRA_FIELDS,BUYERS_DATA,SHARED_WITH_FINANCER,LC_DATA,USER_BANKS} = req.body
    const response = await CRMApplications.create({
      EXPORTER_CODE,
      EXPORTER_NAME,
      APPLICATION_NUMBER,
      APPLICATION_TYPE,
      APPLICATION_STATUS,
      EXTRA_FIELDS:EXTRA_FIELDS ? JSON.parse(EXTRA_FIELDS) :[],
      BUYERS_DATA:BUYERS_DATA ? JSON.parse(BUYERS_DATA) :[],
      SHARED_WITH_FINANCER:SHARED_WITH_FINANCER ? JSON.parse(SHARED_WITH_FINANCER) :[],
      DOCS:docIdArray.length ? docIdArray.join(",") : '',
      USER_BANKS: USER_BANKS ? JSON.parse(USER_BANKS) :[],
      LC_DATA: LC_DATA ? JSON.parse(LC_DATA) :[],

    })
    const responseLogs = await CRMApplicationLogs.create({
      EXPORTER_CODE,
      EXPORTER_NAME,
      APPLICATION_NUMBER,
      APPLICATION_STATUS,
    })

    res.send({
      success:true,
      message:"Application Created Succesfully"
    })
  }catch(e){
    console.log('error in api',e);
    res.send({
      success:false,
      message:"Failed to create application"
    })
  }
}

exports.getCRMApplicationHistory = async(req,res) => {
  try{
    const {APPLICATION_NUMBER} = req.body
    if(APPLICATION_NUMBER){
      const response = await CRMApplicationLogs.aggregate([{
        $match:{
          APPLICATION_NUMBER:APPLICATION_NUMBER
        }
      },{
        $sort:{
          CREATED_AT:-1
        }
      }])
      res.send({
        success:true,
        message:response
      })
    }else{
      res.send({
        success:false,
        message:"Provide an Application Id"
      })
    }
  }catch(e){
    console.log('error in assigntask',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.updateCorporateApplication = async(req,res) => {
  try{
    const {APPLICATION_NUMBER,APPLICATION_STATUS,APPLICATION_REMARK,EXPORTER_CODE,EXPORTER_NAME} = req.body
    if(APPLICATION_NUMBER){
      const responseLogs = await CRMApplicationLogs.create({
        EXPORTER_CODE,
        EXPORTER_NAME,
        APPLICATION_NUMBER,
        APPLICATION_STATUS,
        APPLICATION_REMARK
      })
      await CRMApplications.updateOne({
        APPLICATION_NUMBER:APPLICATION_NUMBER
      },{APPLICATION_STATUS:APPLICATION_STATUS})
      res.send({
        success:true,
        message:'Application Updated Succesfully'
      })
    }else{
      res.send({
        success:false,
        message:"Provide an Application Id"
      })
    }
  }catch(e){
    console.log('error in assigntask',e);
    res.send({
      success:false,
      message:e
    })
  }
}


exports.addNewFinancier = async (req,res) => {
  try{
    let dbFile = []
    let docIdArray = []
    const reqFiles = req.files
    const reqBody = req.body
    if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
            let fileHash = reqFiles[item].md5
            fs.writeFileSync('./docs/' + fileHash, reqFiles[item].data);
            dbFile.push({ name: item, file_name: reqFiles[item].name, hash: fileHash })
        });

        if (dbFile.length) {
            for (let i = 0; i < dbFile.length; i++) {
                let dbReqObj = {
                    "tableName": "tbl_document_details",
                    "insertObj": {
                        doc_no: dbFile[i]["file_name"],
                        doc_name: dbFile[i]["name"].split(":")[0].split("_").join(" "),
                        file_name: dbFile[i]["file_name"],
                        gen_doc_label: dbFile[i]["file_name"],
                        file_hash: dbFile[i]["hash"],
                        valid_upto: "",
                        category: dbFile[i]["name"].split(":")[1] / 1,
                        mst_doc_id: dbFile[i]["name"].split(":")[2] / 1,
                        created_at: new Date(),
                        created_by: reqBody.userId,
                        modified_at: new Date()
                    }
                }
                let dbResObj = await call(dbReqObj, 'setData', 'post');
                if (!dbResObj.success) {
                    console.log('Error while inserting data for cif sale doc:', dbResObj.message)
                    throw errors.databaseApiError;
                }
                let tbl_doc_detail_id = dbResObj.message.dataValues.id
                docIdArray.push(tbl_doc_detail_id)
            }
        }
    }
    const {COMPANY_NAME,CONTACT_PERSON,NAME_TITLE,CONTACT_NUMBER,COUNTRY_CODE,DESIGNATION,EMAIL_ID,SERVICES_OFFERED,EXTRA_FIELDS} = req.body
    const response = await CRMFinanciers.create({
      COMPANY_NAME,
      CONTACT_PERSON,
      NAME_TITLE,
      CONTACT_NUMBER,
      COUNTRY_CODE,
      DESIGNATION,
      EMAIL_ID,
      SERVICES_OFFERED : SERVICES_OFFERED ? JSON.parse(SERVICES_OFFERED): [],
      EXTRA_FIELDS,
      DOCS:docIdArray.length ? docIdArray.join(",") : ''
    })

    res.send({
      success:true,
      message:"Financier Added Succesfully"
    })
  }catch(e){
    console.log('error in api',e);
    res.send({
      success:false,
      message:"Failed to add Financier"
    })
  }
}

exports.getCRMFinanciers = async (req,res) => {
  try{
    const { currentPage, resultPerPage,search} = req.body
    let mainPipeline = []
    if(search){
      mainPipeline.push({
        $match : {
          $or: [
            {
              'COMPANY_NAME': {
                $regex: new RegExp(search),
                $options:'i'
              }
            },
            {
              'CONTACT_NUMBER': {
                $regex: new RegExp(search),
                $options:'i'
              }
            },
            {
              'CONTACT_PERSON': {
               $regex: new RegExp(search),
               $options:'i'
              }
            }
          ]
        }
        
      })
    }
    let countpipline = [...mainPipeline]

    countpipline.push({
      $count:'total_records'
    })
    mainPipeline.push({
      $lookup: {
        from: env === 'dev' ? 'tbl_crm_finaciers_logs' : 'tbl_crm_finaciers_logs_prod' ,
        localField: '_id',
        foreignField: 'id',
        as: 'task_logs'
      }
    })
    mainPipeline.push({
      $project:{
        COMPANY_NAME:1,
        CONTACT_PERSON:1,
        NAME_TITLE:1,
        CONTACT_NUMBER:1,
        COUNTRY_CODE:1,
        DESIGNATION:1,
        EMAIL_ID:1,
        EXTRA_DETAILS:1,
        STATUS:1,
        TASK_ASSIGNED_TO:1,
        SERVICES_OFFERED:1,
        LastNote: {$last: '$task_logs.REMARK'},
        LastEventTime: {$last: '$task_logs.CREATED_AT'},
        LastEventType : {$last: '$task_logs.EVENT_TYPE'},
        LAST_NOTE:'$LastNote',
        LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
        EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
        TASK_DATE:1,  
      }
    })
    
    if(currentPage && resultPerPage) {
      mainPipeline.push({
        '$skip': (currentPage - 1) * parseInt(resultPerPage) 
      })
      mainPipeline.push({
        '$limit': parseInt(resultPerPage) 
      })
    } 
    const response = await CRMFinanciers.aggregate(mainPipeline)
    const countRes = await CRMFinanciers.aggregate(countpipline)
    res.send({
      success:true,
      message:{
        message:response,
        total_records:countRes?.[0]?.total_records || 0
      }
    })
  }catch(e){
    console.log('error in api',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.getCorporateStats = async(req,res) => {
  try{
    const result = await getCorporateStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCorporateStatsFunc = ({taskUpdate,search,dateRangeFilter,onlyShowForUserId,leadAssignedTo,hscodes,leadsStatus,requirements,taskStatus,included_status,taskType}) =>{
  return new Promise(async(resolve,reject)=> {
    try{
      let matchobj  = {}
      if(dateRangeFilter && dateRangeFilter.length >= 1){
        if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
          matchobj = {
            $expr: {
              $eq: [
                { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
                  dateRangeFilter?.[0]  // compare with the target date string
              ]
            }
          }
             
        }else{
          matchobj = {
            'TASK_DATE' :{
              $gte: new Date(dateRangeFilter?.[0]),
              $lte: new Date(dateRangeFilter?.[1])
             }
          }
        }
      }
      
      let mainPipeline = [
      { 
        $match : {
          ...matchobj,
          TASK_TYPE: 'Corporate'
        }
      },
      {
        $match : {
          'TASK_ASSIGNED_TO' : {$exists : true}
        }
      }
      ]
      if(onlyShowForUserId){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.id":onlyShowForUserId
          }
        })
      }
      if(leadAssignedTo && leadAssignedTo.length){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
          }
        })
      }
      let includedTasks = []
      if(taskUpdate?.includes("User Onboarded")){
        if(taskUpdate && taskUpdate.length == 1){
          includedTasks = [4]
        }else{
          includedTasks.push(4)
        }
      }
      if(hscodes && hscodes.length){
        const hsCodesRegex = hscodes.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
        mainPipeline.push({
          $match: {
            "HS_CODES.HS_CODES": { $in: hsCodesRegex }
          }
        });
      }
      if(requirements && requirements.length){
        mainPipeline.push({
          $match: {
            'INTRESTED_SERVICES' : {$in : requirements}
          }
        })
        
      }
  
    
      if(search){
        mainPipeline.push({
          $match:{
            EXPORTER_NAME: {$regex: new RegExp(search) , $options:'i'}
          }
        })
      }
      let applicationPipeline = [...mainPipeline]
      applicationPipeline.push({
        $lookup : {
          from: env === 'dev' ? 'tbl_crm_applications' : 'tbl_crm_applications_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'application_logs'
        }
      })
      applicationPipeline.push({
        $unwind : '$application_logs'
      })
      applicationPipeline.push({
        $project:{
          application_logs: 1
        }
      })
      mainPipeline.push({
        $lookup : {
          from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'task_logs'
        }
      })
      
      mainPipeline.push(
        taskType === 'Exporter Wise'? {
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            },
            STATUS:1,
            TASK_ASSIGNED_TO:1,
            LAST_NOTE:1,
            LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
            EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
            TASK_DATE:1,
            EVENT_TIME:{$last: '$task_logs.EVENT_TIME'},
            EXPORTER_CODE:1
          }
        } :{
          '$unwind': {
              'path': '$task_logs', 
              'includeArrayIndex': 'i', 
              'preserveNullAndEmptyArrays': true
          }
        },
      )
      if(taskType === 'Task Wise'){
        mainPipeline.push({
          $project: {
            EVENT_STATUS : '$task_logs.EVENT_STATUS',
            STATUS : 1,
            LOG_TYPE:'$task_logs.LOG_TYPE',
            TASK_DATE:1 ,
            EVENT_TIME:'$task_logs.EVENT_TIME',
            EXPORTER_CODE:1
          }
        })
      }
      if(taskStatus && taskStatus.length){
        mainPipeline.push({
          $match: {
            'EVENT_STATUS' : {
              $in : taskStatus.map(item => new RegExp(item))
            }
          }
        })
      } 
      if(leadsStatus && leadsStatus.length){
        if(leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
        }
      }
      if(taskUpdate){
        let statusArray = taskUpdate.filter(element => element !== 'User Onboarded' && element !== 'Lead Created')
        if(statusArray && statusArray.length ){
          mainPipeline.push({
            $match:{
              $or : [
                {
                  'STATUS': {
                    '$in': includedTasks
                  }
                },
                {$and : [
                  {'LOG_TYPE' : 'Didnt connect'},
                  {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
                },
                {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
              ]
            }
          })
        }else{
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
        }
        
      }else{
        if(!leadsStatus){
          console.log('hereeee',included_status);
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
        }
      }
      const logTypePipeline = [...mainPipeline]
      
      logTypePipeline.push({
        $group : {
          _id: '$LOG_TYPE',
          'total_records' : {$sum: 1},
          'LOG_TYPE':{$first:'$LOG_TYPE'}
        }
      })
      mainPipeline.push({
        $group : {
          _id: '$EVENT_STATUS',
          'total_records' : {$sum: 1},
          'EVENT_TYPE':{$first : '$EVENT_STATUS'}
        }
      })

      const eventResponse = await ExporterModelV2.aggregate(mainPipeline)
      const logsResponse = await ExporterModelV2.aggregate(logTypePipeline)
      console.log('dataaaaaaa',JSON.stringify(applicationPipeline));
      const applicationResponse = await ExporterModelV2.aggregate(applicationPipeline)
      let lc_count = 0
      let inv_count = 0
      let others_count = 0
      let total_count = 0
      for(let i =0; i<= applicationResponse.length- 1; i++){
        total_count+=1
        let element = applicationResponse[i].application_logs
        console.log('elementtttt',element);
        if(element.APPLICATION_TYPE?.includes('LC')){
          lc_count += 1
        }else if(element.APPLICATION_TYPE?.includes('Invoice')){
          inv_count += 1
        }else{
          others_count += 1
        }
      }
      let finalArry = []
      for(let i = 0; i<= eventResponse.length - 1; i++){
        let element = eventResponse[i]
        if(element.EVENT_TYPE?.includes("Hot")){
          finalArry.push({
            ...element,
            label: "Hot"
          })
        }else if(element.EVENT_TYPE?.includes("Cold")){
          finalArry.push({
            ...element,
            label: "Cold"
          })
        }else if(element.EVENT_TYPE?.includes("Warm")){
          finalArry.push({
            ...element,
            label: "Warm"
          })
        }
      }
      for(let i = 0; i<= logsResponse.length - 1; i++){
        let element = logsResponse[i]
        if(element?.LOG_TYPE == "Not Interested"){
          finalArry.push({
            ...element,
            label: "Not Interested"
          })
        }else if(element.LOG_TYPE === 'Lead Lost'){
          finalArry?.push({
            ...element,
            label: "Lead Lost"
          })
        }
      }
      resolve({
        success:true,
        message:{
          statsArr :finalArry,
          applicationStats :{
            lc_count,
            inv_count,
            others_count,
            total_count
          }
        }
      })
    }catch(e){
      console.log('error in api corporate statss',e);
      reject({
        success:false
      })
    }
  })
}

exports.getMonthlyGoalsProgress = async (req,res) => {
  try{
    const {userId} = req.body
    const firstDayOfMonth = moment().startOf('month').format('YYYY-MM-DD');
    const lastDayOfMonth = moment().endOf('month').format('YYYY-MM-DD');
    const goalsQuery = `SELECT * FROM  tbl_subadmin_goals WHERE tbl_user_id='${userId}'`
    const goalsRes = await call({query:goalsQuery},'makeQuery','get')
    if(goalsRes?.message?.length){
      const goalsObj = goalsRes?.message?.[0] || {}
      const invDisQuery = `SELECT SUM(tbl_disbursement_scheduled.amount) as totalDisbursedAmount 
      FROM tbl_disbursement_scheduled 
      INNER JOIN tbl_invoice_discounting ON 
      tbl_disbursement_scheduled.invRefNo = tbl_invoice_discounting.reference_no
      INNER JOIN tbl_user ON 
      tbl_invoice_discounting.seller_id = tbl_user.id
      WHERE tbl_user.LeadAssignedTo = '${userId}' AND tbl_disbursement_scheduled.status = 1 AND tbl_disbursement_scheduled.scheduledOn >= '${firstDayOfMonth}' AND tbl_disbursement_scheduled.scheduledOn <= '${lastDayOfMonth}'`
      const invDisRes = await call({query:invDisQuery},'makeQuery','get')
      let invPer = (((invDisRes?.message?.[0]?.totalDisbursedAmount || 0 ) / goalsObj.invDis_goal) * 100)
  
      const lcDisQuery = `SELECT SUM(tbl_disbursement_scheduled.amount) as totalDisbursedAmount 
      FROM tbl_disbursement_scheduled 
      INNER JOIN tbl_buyer_required_lc_limit ON 
      tbl_disbursement_scheduled.invRefNo = tbl_buyer_required_lc_limit.id
      INNER JOIN tbl_user ON 
      tbl_buyer_required_lc_limit.createdBy = tbl_user.id
      WHERE tbl_user.LeadAssignedTo = '${userId}' AND tbl_disbursement_scheduled.status = 1 AND tbl_disbursement_scheduled.scheduledOn >= '${firstDayOfMonth}' AND tbl_disbursement_scheduled.scheduledOn <= '${lastDayOfMonth}'`
      const lcDisRes = await call({query:lcDisQuery},'makeQuery','get')
      let lcPer = (((lcDisRes?.message?.[0]?.totalDisbursedAmount || 0 ) / goalsObj.lcDis_goal) * 100)
  
      const userOnboardedQuery = `SELECT COUNT(id) as total_users FROM tbl_user WHERE tbl_user.LeadAssignedTo = '${userId}' AND tbl_user.created_at >= '${firstDayOfMonth}' AND tbl_user.created_at <= '${lastDayOfMonth}'`
      const userOnboardRes = await call({query:userOnboardedQuery},'makeQuery','get')
      let userOnboardPer = (((userOnboardRes?.message?.[0]?.total_users || 0 ) / goalsObj.onboarding_goal) * 100)
  
      const buyersAddedQuery = `SELECT COUNT(tbl_buyers_detail.id) as total_buyers FROM tbl_buyers_detail
        LEFT JOIN tbl_user ON tbl_user.id = tbl_buyers_detail.user_id 
        WHERE tbl_user.LeadAssignedTo = '${userId}' AND tbl_buyers_detail.created_at >= '${firstDayOfMonth}' AND tbl_buyers_detail.created_at <= '${lastDayOfMonth}'
      `
      const buyerAddedRes = await call({query:buyersAddedQuery},'makeQuery','get')
      let buyerAddedPer = (((buyerAddedRes?.message?.[0]?.total_buyers || 0 ) / goalsObj.buyeradded_goal) * 100)
  
      res.send({
        success:true,
        message: {
          invPer:Math.round(invPer),
          lcPer:Math.round(lcPer),
          userOnboardPer:Math.round(userOnboardPer),
          buyerAddedPer: Math.round(buyerAddedPer)
        }
      })
    }else{
      res.send({
        success:false,
        message:'Goals Not Set'
      })
    }
    
  }catch(e){
    res.send({
      success:false,
      message:e
    })
  }
}

exports.addCRMFinancers = async (req,res) => {
  try{
    const result = await addCRMFinancersFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const addCRMFinancersFunc = ({ taskDate,HS_CODE }, reqFiles) => {``
  return new Promise(async(resolve, reject) => {
    try {
      let filepath = ''
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
        const result = [];
        for (let i = 0; i <= data.length - 1; i++) {
          let obj = data[i]
          let contactPerson = obj["Contact Person"]?.split("/")[0]
         
          const [nameTitle, personName] = contactPerson?.split('.') || ["", ""]
          const firstEmail = obj["Email Id"]?.split(";")[0]
          const contactNumber = typeof (obj["Contact Number"]) === 'string' ?  parseInt(obj["Contact Number"]?.split("/")[0]) : obj["Contact Number"]
          result.push({
            COMPANY_NAME : obj["FI Name"],
            CONTACT_PERSON: personName?.trim() || "",
            NAME_TITLE :nameTitle,
            DESIGNATION : obj["Designation"],
            EMAIL_ID:firstEmail || "",
            SERVICES_OFFERED:obj["Product"]?.split(",") || [],
            CONTACT_NUMBER: contactNumber || "",
            LastNote:obj["Discussion Point / Way Forward"]
          }) 
        }
        await CRMFinanciers.insertMany(result)
        fs.unlinkSync(filepath)
        resolve({
          success:true,
          message:'Financers added succesfully',
          Failed:[]
        })
      }

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}

exports.addCorporateExporters = async (req,res) => {
  try{
    const result = await addCorporateExportersFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const addCorporateExportersFunc = ({ taskDate,HS_CODE }, reqFiles) => {``
  return new Promise(async(resolve, reject) => {
    try {
      let filepath = ''
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        console.log('sheet namesssss',sheet_name_list);
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
        const result = [];
        let codes = []
        let cnt =0
        let not_added = []

        for (let i = 0; i <= data.length - 1; i++) {
          let obj = data[i]
          const res = await ExporterModelV2.aggregate([{
            $match: {
              'EXPORTER_NAME': {
                $regex: new RegExp(obj.EXPORTER_NAME),
                $options: 'i'
              }
            }
          }])
          if(res && res.length > 0){
            codes.push(res[0].EXPORTER_CODE)
            result.push({
              EXPORTER_CODE : res[0].EXPORTER_CODE
            })
          }else{
            not_added.push(obj.EXPORTER_NAME)
          }
         
        }
        console.log('total exporters',result.length)
        let reqObj = {
          "AssignmentObject": {
              "('1186')": {
                  "EXPORTER_CODE": result,
                  "selectedTask": "Corporate"
              }
          },
          "STATUS" : 0,
          'FOLDER_NAME':'Corporate Callings',
          'ASSIGNEE_NAME':'Admin',
          'ASSIGNEE_ID':1,
          'FILTERS' : []
        }
      
        const response = []

        const res = await AssignMasterBulkDataTaskFunc(reqObj)
        fs.unlinkSync(filepath)
        resolve({
          ...response,
          not_added
        })
      }

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}


exports.updateFinancierTask = async (req,res) => {
  try{
    const result =await updateFinancierTaskFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const updateFinancierTaskFunc = ({id,COMPANY_NAME,EVENT_TYPE,EVENT_STATUS,EVENT_TIME,REMINDER,REMARK,ASSIGN_TASK_TO,LOG_TYPE,LOST_REASON,CONTACT_PERSON,CONTACT_NUMBER,ADMIN_ID,ADMIN_NAME}) => {
  return new Promise(async(resolve,reject) => {
    try{
        if(LOG_TYPE === 'Create New Task'){
          await CRMFinanciers.findOneAndUpdate({_id:id},{TASK_ASSIGNED_TO : ASSIGN_TASK_TO,REMINDER:REMINDER,TASK_DATE:EVENT_TIME})
          await CRMFinLogs.create({
            id,
            COMPANY_NAME,
            EVENT_TYPE,
            EVENT_STATUS,
            EVENT_TIME,
            REMARK,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'Didnt connect'){
          await CRMFinanciers.findOneAndUpdate({_id:id},{TASK_ASSIGNED_TO : ASSIGN_TASK_TO})
          await CRMFinLogs.create({
            id,
            COMPANY_NAME,
            EVENT_STATUS,
            REMARK,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER
          })
        }else if(LOG_TYPE === 'Call back'){
          await CRMFinanciers.findOneAndUpdate({_id:id},{TASK_ASSIGNED_TO : ASSIGN_TASK_TO,REMINDER:REMINDER,TASK_DATE:EVENT_TIME})
          await CRMFinLogs.create({
            id,
            COMPANY_NAME,
            EVENT_STATUS,
            REMARK,
            EVENT_TIME,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'Not Interested'){
          await CRMFinanciers.findOneAndUpdate({_id:id},{STATUS:2,TASK_DATE:EVENT_TIME})
          await CRMFinLogs.create({
            id,
            COMPANY_NAME,
            EVENT_STATUS,
            REMARK,
            LOG_TYPE,
            EVENT_TIME,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }
        else if(LOG_TYPE === 'Lead Lost'){
          await CRMFinanciers.findOneAndUpdate({_id:id},{STATUS:3})
          await CRMFinLogs.create({
            id,
            COMPANY_NAME,
            EVENT_STATUS,
            REMARK,
            LOG_TYPE,
            LOST_REASON,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'Lead Created'){
          await CRMFinanciers.findOneAndUpdate({_id:id},{STATUS:1,TASK_ASSIGNED_TO : ASSIGN_TASK_TO,REMINDER:REMINDER,TASK_DATE:EVENT_TIME})
          await CRMFinLogs.create({
            id,
            COMPANY_NAME,
            EVENT_TYPE,
            EVENT_STATUS,
            EVENT_TIME,
            REMARK,
            LOG_TYPE,
            CONTACT_PERSON,
            CONTACT_NUMBER,
            ADMIN_ID,
            ADMIN_NAME
          })
        }else if(LOG_TYPE === 'User Onboarded'){
          await CRMFinanciers.findOneAndUpdate({_id:id},{STATUS:4})
          await CRMFinLogs.create({
            id,
            COMPANY_NAME,
            LOG_TYPE,
            ADMIN_ID,
            ADMIN_NAME
          })
        }
        
        resolve({
          success:true,
          message:'Task Created Succesfully'
        })
      }catch(e){
        console.log('error in API', e);
        reject({
          success:false,
          message:'Task Creation Failed'
        })
    }
  })
}

exports.getFinCallHistory = async(req,res) => {
  try{
    const {id} = req.body
    if(id){
      const response = await CRMFinLogs.aggregate([{
        $match:{
          id: new ObjectId(id)
        }
      },{
        $sort:{
          CREATED_AT:-1
        }
      }])
      res.send({
        success:true,
        message:response
      })
    }else{
      res.send({
        success:false,
        message:"Provide an User Id"
      })
    }
  }catch(e){
    console.log('error in assigntask',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.exportExtraExcel = async (req,res) => {
  try{
    const result = await exportExtraExcelFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const exportExtraExcelFunc = ({ taskDate,HS_CODE }, reqFiles) => {``
  return new Promise(async(resolve, reject) => {
    try {
      let filepath = ''
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        console.log('sheet namesssss',sheet_name_list);
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
        const result = [];
        let codes = []
        let cnt =0
        let finaldata = []
        for (let i = 0; i <= data.length - 1; i++) {
          let obj = data[i]
          result.push(obj.EXPORTER_NAME)
          const exporters = await ExporterModel.aggregate([
            {
              $match:{
                $or:[
                  {
                    $and: [
                     { EXPORTER_NAME : obj.EXPORTER_NAME},
                     { EXPORTER_ADDRESS: {
                        $regex: new RegExp('morbi'),
                        $options: 'i'
                      }}
                    ]
                  },
                  {EXPORTER_NAME:obj.EXPORTER_NAME}
                ]
                
              }
            },
            {
              $lookup: {
                from: 'tbl_crm_tasks_prod',
                localField: 'EXPORTER_CODE',
                foreignField:'EXPORTER_CODE',
                as:'crm_details'
              }
            }
          ])
          const firstExporter = exporters[0]
          const groupedArray = firstExporter?.HS_CODES?.reduce((groups, obj) => {
            const { HS_CODES } = obj;
            const firstTwoDigits = HS_CODES.substring(0, 2);

            if (!groups[firstTwoDigits]) {
              groups[firstTwoDigits] = [];
            }

            groups[firstTwoDigits].push(obj);
            return groups;
          }, {});
          const keys = Object.keys(groupedArray || {});
          const hsRes = keys.map(key => ({ HS_CODE: key }));
          console.log(hsRes);
          let exportObj = {
            SR_NO : i+1 ,
            EXPORTER_CODE:firstExporter?.EXPORTER_CODE,
            EXPORTER_NAME:firstExporter?.EXPORTER_NAME || obj.EXPORTER_NAME,
            EXPORTER_ADDRESS:firstExporter?.EXPORTER_ADDRESS || obj.EXPORTER_ADDRESS,
            TOTAL_BUYERS: firstExporter?.BUYERS?.length || 0,
            EXPORTER_CITY:obj.EXPORTER_CITY,
            FOB:firstExporter?.FOB || 0,
            "HS Code": hsRes.map(item => item.HS_CODE).join(","),
          
          }
          if(firstExporter?.crm_details?.[0]?.EXTRA_DETAILS?.[0]){
            const extra_obj = firstExporter?.crm_details?.[0]?.EXTRA_DETAILS?.[0]
            exportObj = {
              ...exportObj,
              Department:extra_obj["Department"] || "",
              "Contact Person": extra_obj["Contact Person"] || "",
              Designation:extra_obj["Designation"] || "",
              DIN:extra_obj["DIN"] || "",
              "GST/ Establishment Number":extra_obj["GST/ Establishment Number"] || "",
              "Contact Number":extra_obj["Contact Number"] || "",
              "Email ID":extra_obj["Email ID"] || ""
            }
          }
          finaldata.push(exportObj)
          if(firstExporter?.crm_details?.[0]?.EXTRA_DETAILS?.length){
            const EXTRA_DETAILS = firstExporter?.crm_details?.[0]?.EXTRA_DETAILS
            for(let j = 1; j<= EXTRA_DETAILS.length - 1 ; j++){
              let extra_obj = EXTRA_DETAILS[j]
              let exportObj = {
                SR_NO : "",
                EXPORTER_CODE:"",
                EXPORTER_NAME:"",
                EXPORTER_ADDRESS:"",
                EXPORTER_CITY:"",
                TOTAL_BUYERS: "",
                FOB:"",
                "HS Code": "",
                Department:extra_obj["Department"] || "",
                "Contact Person": extra_obj["Contact Person"] || "",
                Designation:extra_obj["Designation"] || "",
                DIN:extra_obj["DIN"] || "",
                "GST/ Establishment Number":extra_obj["GST/ Establishment Number"] || "",
                "Contact Number":extra_obj["Contact Number"] || "",
                "Email ID":extra_obj["Email ID"] || ""
              }
              finaldata.push(exportObj)

            }
          } 
        }
        const ws = XLSX.utils.json_to_sheet(finaldata)
        const wb = { Sheets: { data: ws }, SheetNames: ['data'] }
        XLSX.writeFile(wb,"iPHEX_Contacts_List.xlsx",{bookType:'xlsx',type:'array'})
        const excelbuffer = XLSX.write(wb, { bookType: 'xlsx', type: 'array' })
        console.log('resultttt',result.length);
        fs.unlinkSync(filepath)
        resolve({
          success:true
        })
      }

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}

exports.getLocationSearch = async(req,res) => {
  try{
    const url = `https://api.mapbox.com/search/searchbox/v1/suggest?q=${req.body.search}?&session_token=231&access_token=${process.env.MAPBOX_TOKEN}`
    const response = await axios.get(url)
    return res.send({
      success:true,
      message: response.data?.suggestions || []
    })
  }catch(e){
    return res.send({
      success:false,
      message: e
    })
  }
}


exports.getHSTrendGraphV2 = async(req,res) => {
  try{
    const result = await getHSTrendGraphV2Func(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getHSTrendGraphV2Func = ({HSTrendGraphDuration,priceHistoryFrom,priceHistoryTo,selectedHS,searchParam,HS_CODES,showImports,country_name,EXPORTER_NAMES,EXPORTER_NAME,EXPORTER_COUNTRY}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths 
      if(HSTrendGraphDuration){
        countForMonths = HSTrendGraphDuration?.split(" ")[0] / 1
      }else{
        countForMonths = moment(priceHistoryTo).diff(priceHistoryFrom,'month')
      }
      let dateFormat = ''
      if(countForMonths > 12){
        dateFormat = '%Y'
      }else if(countForMonths > 3){
        dateFormat = '%Y-%m-01'
      }else if(countForMonths === 1){
        dateFormat = '%Y-%m-%d'
      }else{
        dateFormat = "W%V"
      }
      let toppipeline = []
      let searchObj = {}
      let hs_len = {
        $substr: ["$HS_CODE",0,6]
      }
      let hsnt5len =  {
        $substr: ['$HS_CODES.HS_CODES',0,6]
      }
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        toppipeline.push({
          $match : {
            $and : [
              searchObj,
              EXPORTER_NAMES && EXPORTER_NAMES.length? {'EXPORTER_NAME': {$in: EXPORTER_NAMES}} :{},
            ]
          }
        })
        hs_len = isNaN(parseInt(searchParam)) ? {
          $substr: ["$HS_CODE",0,2]
        } : {
          $substr: ["$HS_CODE",0,6]
        }
        hsnt5len = isNaN(parseInt(searchParam)) ? {
          $substr:['$HS_CODES.HS_CODES',0,2]
        } : {
          $substr: ['$HS_CODES.HS_CODES',0,6]
        }
      }
      let condobj = {}
      if(EXPORTER_NAME){
        condobj = {
          EXPORTER_NAME : EXPORTER_NAME
        }
      }
      // if(EXPORTER_COUNTRY){
      //   condobj = {
      //     ...condobj,
      //     EXPORTER_COUNTRY : EXPORTER_COUNTRY
      //   }
      // }
      toppipeline.push({
        '$match': {
          ...condobj
        }
      })
      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
        }
      }

      if(selectedHS){
          toppipeline.push({
            '$match': {
              'HS_CODES.HS_CODES': {
                $regex : new RegExp("^" + selectedHS) 
              }
            }
          })
          hs_len = {
            $substr: ["$HS_CODE",0,6]
          }
          hsnt5len =  {
            $substr: ['$HS_CODES.HS_CODES',0,6]
          }
      }
      toppipeline.push({
        '$unwind': {
          'path': '$HS_CODES', 
          'includeArrayIndex': 'data', 
          'preserveNullAndEmptyArrays': true
        }
      })
      if(selectedHS){
        toppipeline.push({
          '$match': {
            'HS_CODES.HS_CODES': {
              $regex : new RegExp("^" + selectedHS) 
            }
          }
        })
      }
      if(HS_CODES && HS_CODES.length){
        toppipeline.push({
          '$match': {
            "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
          }
        })
      }
      toppipeline = [...toppipeline,{
        '$project': {
          'HS_CODES': 1
        }
      }, {
        '$group': {
          '_id': hsnt5len, 
          'FOB': {
            '$sum': '$HS_CODES.FOB_VALUE_USD'
          }
        }
      }, {
        '$sort': {
          'FOB': -1
        }
      }, {
        '$limit': 5
      }, {
        '$project': {
          '_id': 0, 
          'HS_CODES': '$_id', 
          'FOB': '$FOB'
        }
      }]
      const top5HS = await ExporterModelV2.aggregate(toppipeline)

      let pipelinedata = []
      let mainSearchObj = {}
      if(searchParam){
        mainSearchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        pipelinedata.push({
          $match: {
            $and : [
              mainSearchObj,
              EXPORTER_NAMES && EXPORTER_NAMES.length ? {'EXPORTER_NAME': {$in: EXPORTER_NAMES}} :{},
              // country_name ? {[showImports ? 'DESTINATION_COUNTRY' : 'EXPORTER_COUNTRY'] : country_name} : {}
            ]
          }        
        })
      }
      if(EXPORTER_NAME){
        condobj = {
          EXPORTER_NAME : EXPORTER_NAME
        }
      }
      pipelinedata.push({
        '$match': {
          ...condobj
        }
      })
      if(priceHistoryFrom && priceHistoryTo){
        pipelinedata.push({ 
          $match : {
           'DATE' :{
            $gte: new Date(priceHistoryFrom),
            $lte: new Date(priceHistoryTo)
           }
          }
        })
      }
      pipelinedata = [ ...pipelinedata,
        {
          $match: {
            'HS_CODE': {
              $in : top5HS.map(item => new RegExp(`^${item.HS_CODES}`))
            }
          }
        },
        {
          $group: {
            '_id': {
                DATE_STRING: {
                $dateToString: { format: dateFormat, date: "$DATE" }
              },
              HS_CODE:hs_len
            },
            AVG_PRICE_USD: {$avg: '$UNIT_PRICE_USD'}
          }
        },
        {
          $group: {
            _id: '$_id.DATE_STRING',
            'HS_CODES': {
                '$push': {
                  'HS_CODE': '$_id.HS_CODE',
                  'AVG_PRICE_USD': '$AVG_PRICE_USD'
                }
            }
          } 
        },{
          $project : {
            _id:0,
            HS_CODES:1,
            label:"$_id"
          }
        },
        {
          $sort: {
            'label':1
          }
        }
      ]
      
      const response = await TTV.aggregate(pipelinedata)

      let finaldata =[]
      response.forEach(item => {
        let finalobj ={}
        finalobj.label = item.label
        
        item.HS_CODES.forEach(element => {
            finalobj[`${element.HS_CODE}`] = parseFloat(element.AVG_PRICE_USD?.toFixed(2)) 
        }) 
        finaldata.push(finalobj)
      })
      let chartconfig= []
      top5HS.forEach((element,index) => {
        chartconfig.push({
          dataKey:`${element.HS_CODES}`,
          fill:hsncolors[index],
          display:element.HS_CODES
        })
      })
      resolve({
        success:true,
        message:{
          message:finaldata,
          chartconfig,
        }
      })
    }catch(e){
      console.log('error in HScgraph',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}


exports.getHSExportTrendGraphV2 = async(req,res) => {
  try{
    const result = await getHSExportTrendGraphV2Func(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getHSExportTrendGraphV2Func = ({HSTrendGraphDuration,priceHistoryFrom,priceHistoryTo,EXPORTER_CODE,selectedHS,searchParam,HS_CODES,EXPORTER_CODES,showImports,country_name,EXPORTER_NAMES,EXPORTER_NAME,EXPORTER_COUNTRY}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths 
      if(HSTrendGraphDuration){
        countForMonths = HSTrendGraphDuration?.split(" ")[0] / 1
      }else{
        countForMonths = moment(priceHistoryTo).diff(priceHistoryFrom,'month')
      }
      let dateFormat = ''
      if(countForMonths > 12){
        dateFormat = '%Y'
      }else if(countForMonths > 3){
        dateFormat = '%Y-%m-01'
      }else if(countForMonths === 1){
        dateFormat = '%Y-%m-%d'
      }else{
        dateFormat = "W%V"
      }
      let toppipeline = []
      let searchObj = {}
      let hs_len = {
        $substr: ["$HS_CODE",0,6]
      }
      let hsnt5len =  {
        $substr: ['$HS_CODES.HS_CODES',0,6]
      }
      if(searchParam){
        searchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODES.HS_CODES": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        toppipeline.push({
          $match : {
            $and : [
              searchObj,
              EXPORTER_NAMES && EXPORTER_NAMES.length? {'EXPORTER_NAME': {$in: EXPORTER_NAMES}} :{},
            ]
          }
        })
        hs_len = isNaN(parseInt(searchParam)) ? {
          $substr: ["$HS_CODE",0,2]
        } : {
          $substr: ["$HS_CODE",0,6]
        }
        hsnt5len = isNaN(parseInt(searchParam)) ? {
          $substr:['$HS_CODES.HS_CODES',0,2]
        } : {
          $substr: ['$HS_CODES.HS_CODES',0,6]
        }
      }
      let condobj = {}

      if(EXPORTER_NAME){
        condobj = {
          EXPORTER_NAME : EXPORTER_NAME
        }
      }
      if(EXPORTER_COUNTRY){
        condobj = {
          ...condobj,
          EXPORTER_COUNTRY : EXPORTER_COUNTRY
        }
      }
      toppipeline.push({
        '$match': {
          ...condobj
        }
      })

      if(HS_CODES && HS_CODES.length){
        hsObj = { 
          "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
        }
      }

      if(selectedHS){
          toppipeline.push({
            '$match': {
              'HS_CODES.HS_CODES': {
                $regex : new RegExp("^" + selectedHS) 
              }
            }
          })
          hs_len = {
            $substr: ["$HS_CODE",0,6]
          }
          hsnt5len =  {
            $substr: ['$HS_CODES.HS_CODES',0,6]
          }
      }
      toppipeline.push({
        '$unwind': {
          'path': '$HS_CODES', 
          'includeArrayIndex': 'data', 
          'preserveNullAndEmptyArrays': true
        }
      })
      if(selectedHS){
        toppipeline.push({
          '$match': {
            'HS_CODES.HS_CODES': {
              $regex : new RegExp("^" + selectedHS) 
            }
          }
        })
      }
      if(HS_CODES && HS_CODES.length){
        toppipeline.push({
          '$match': {
            "HS_CODES.HS_CODES": { '$in': HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))} 
          }
        })
      }
     
      toppipeline = [...toppipeline,{
        '$project': {
          'HS_CODES': 1
        }
      }, {
        '$group': {
          '_id':hsnt5len, 
          'FOB': {
            '$sum': '$HS_CODES.FOB_VALUE_USD'
          }
        }
      }, {
        '$sort': {
          'FOB': -1
        }
      }, {
        '$limit': 5
      }, {
        '$project': {
          '_id': 0, 
          'HS_CODES': '$_id', 
          'FOB': '$FOB'
        }
      }]
      const top5HS = await ExporterModelV2.aggregate(toppipeline)
      let pipelinedata = []
      let mainSearchObj = {}

      if(searchParam){
        mainSearchObj = isNaN(parseInt(searchParam)) ? {
          'EXPORTER_NAME': {
            $regex: new RegExp(`${searchParam}`),
            $options:'i'
          }
        } : { 
          "HS_CODE": { '$regex': new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)} 
        }
        pipelinedata.push({
          $match: {
            $and : [
              mainSearchObj,
              EXPORTER_NAMES && EXPORTER_NAMES.length ? {'EXPORTER_NAME': {$in: EXPORTER_NAMES}} :{},
              // country_name ? {[showImports ? 'DESTINATION_COUNTRY' : 'EXPORTER_COUNTRY'] : country_name} : {}
            ]
          }
        })
      }
      if(EXPORTER_NAME){
        condobj = {
          EXPORTER_NAME : EXPORTER_NAME
        }
      }

      console.log('condobj',condobj,EXPORTER_NAME);
      pipelinedata.push({
        '$match': {
          ...condobj
        }
      })
      if(priceHistoryFrom && priceHistoryTo){
        pipelinedata.push({ 
          $match : {
           'DATE' :{
            $gte: new Date(priceHistoryFrom),
            $lte: new Date(priceHistoryTo)
           }
          }
        })
      }
      
      pipelinedata = [ ...pipelinedata,
        {
          $match: {
            'HS_CODE': {
              $in : top5HS.map(item => new RegExp(`^${item.HS_CODES}`))
            }
          }
        },
        {
          $project: {
            DATE_STRING: { $dateToString: { format: dateFormat, date: "$DATE" } },
            HS_CODE: hs_len,
            quantity_divided: {
              $cond: {
                if: { $eq: ["$UNIT_PRICE_USD", 0] },
                then: 0, // or you can set it to some other default value
                else: { $divide: ["$FOB_VALUE_USD", "$UNIT_PRICE_USD"] }
              }
            },
            FOB_VALUE_USD: 1, // Include other fields in the output if needed
            AVG_PRICE_PER_UNIT_USD: '$UNIT_PRICE_USD' // Include other fields in the output if needed
          }
        },
        {
          $group: {
            _id: {
              DATE_STRING: "$DATE_STRING",
              HS_CODE: "$HS_CODE"
            },
            Quantity: { $avg: "$quantity_divided" }, // Calculate the average of "quantity_divided"
            FOB: { $sum: "$FOB_VALUE_USD" }
          }
        },
        {
          $group: {
            _id: '$_id.DATE_STRING',
            'HS_CODES': {
                '$push': {
                  'HS_CODE': '$_id.HS_CODE',
                  'Quantity': '$Quantity',
                  'FOB': '$FOB'
                }
            }
          } 
        },{
          $project : {
            _id:0,
            HS_CODES:1,
            label:"$_id"
          }
        },
        {
          $sort: {
            'label':1
          }
        }
      ]
      console.log('pipelinedata',JSON.stringify(pipelinedata)); 
      const response = await TTV.aggregate(pipelinedata)

      let finaldata =[]
      response.forEach(item => {
        let finalobj ={}
        finalobj.label = item.label
        
        item.HS_CODES.forEach(element => {
            finalobj[`${element.HS_CODE}_VALUE`] = element.FOB
            finalobj[`${element.HS_CODE}_QUANTITY`] = parseFloat(element.Quantity?.toFixed(2))
        }) 
        finaldata.push(finalobj)
      })
      let chartconfig= []
      let  quantitychartconfig =[]
      top5HS.forEach((element,index) => {
        chartconfig.push({
          dataKey:`${element.HS_CODES}_VALUE`,
          fill:hsncolors[index],
          display:element.HS_CODES
        })
        quantitychartconfig.push({
          dataKey:`${element.HS_CODES}_QUANTITY`,
          fill:hsncolors[index],
          display:element.HS_CODES
        })

      })
      resolve({
        success:true,
        message:{
          message:finaldata,
          chartconfig,
          quantitychartconfig,
        }
      })
    }catch(e){
      console.log('error in HScgraph',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getCRMMasterdataV2 = async(req,res) => {
  try{
    //const result = await getCRMMasterdataFunc (req.body)
    const {search, country_name, currentPage, resultPerPage, searchParam, HS_CODES, AVAILABLE_CONTACTS, TURNOVER_RANGE, CITIES, STATUS, ORGANIZATION_TYPE, companyName, contactPerson, contactNo, designation, sortBuyerCount, sortCity, sortCompanyName, sortContactPerson, sortTurnover, leadAssignedTo, sortleadAssigned, BUYERS, COUNTRIES,EXPORTER_CODES,showImports,EXPORTER_NAMES } = req.body   
    //res.send(result)
    const pipelinedata = [];
    let FOB_BY_HS = null


    if (companyName && companyName.length) {
      pipelinedata.push({
        $match: {
          [showImports ? 'BUYER_NAME' : 'EXPORTER_NAME']: { $in: companyName }
        }
      });
    }
  
    if (HS_CODES && HS_CODES.length) {
      const hsCodesRegex = HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
      pipelinedata.push({
        $match: {
          "HS_CODES.HS_CODES": { $in: hsCodesRegex }
        }
      });
      FOB_BY_HS = {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$HS_CODES",
                as: "code",
                cond: {
                  $in: [
                    { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                    HS_CODES
                  ]
                }
              }
            },
            as: "code",
            in: "$$code.FOB_VALUE_USD"
          }
        }
      } 
    }
    if(searchParam){
      if(!isNaN(parseInt(searchParam))){
        FOB_BY_HS = {
          $sum: {
            $map: {
              input: {
                $filter: {
                  input: "$HS_CODES",
                  as: "code",
                  cond: {
                    $regexMatch: {
                      input: "$$code.HS_CODES",
                      regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`)
                    }
                  }
                }
              },
              as: "code",
              in: "$$code.FOB_VALUE_USD"
            }
          }
        }
      }
    }
    let  organiztionType = {}
    if(ORGANIZATION_TYPE && ORGANIZATION_TYPE.length >= 1){
      let newObj = []
      if(ORGANIZATION_TYPE.includes("Others") && ORGANIZATION_TYPE.length > 1){
        newObj.push({
          'EXPORTER_NAME':{
            $regex: new RegExp(ORGANIZATION_TYPE.filter(item => item !== 'Others').join("|")), $options:'i'
          }
        })
        newObj.push({
          'EXPORTER_NAME': { $not: {$regex:/PVT LTD|PUB LTD|LLP/,$options:'i'}}
        })
      }else if(ORGANIZATION_TYPE.includes("Others")){
        newObj.push({
          'EXPORTER_NAME': {
            $not: /pvt|pub|llp/i
          }
        })
      }else{
        newObj.push({
          'EXPORTER_NAME': {
            $regex:new RegExp(ORGANIZATION_TYPE.filter(item => item !== 'Others').join("|"),'i') , 
          }
        })
      }
      
      organiztionType = {
        $or : newObj
      }
    }

    let projectStage = {
      $project : {
        EXPORTER_NAME: showImports ? '$BUYER_NAME' : '$EXPORTER_NAME',
        EXPORTER_ADDRESS: showImports ? '$BUYER_ADDRESS' : '$EXPORTER_ADDRESS',
        FOB: 1,
        EXPORTER_CODE: showImports ? '$BUYER_CODE' : '$EXPORTER_CODE',
        EXPORTER_CITY: showImports ? '$BUYER_CITY' : '$EXPORTER_CITY',
        EXTRA_DETAILS: 1,
        TASK_ASSIGNED_TO:1,
        TOTAL_BUYERS: {
          $size: {
             $ifNull: [showImports ? '$EXPORTERS' : '$BUYERS', []]
           }
        },
        BUYERS:showImports ? '$EXPORTERS' : '$BUYERS',
        STATUS:1,
        HS_CODES:1,
        CIN_NO:1,
        AUDITOR_DATA:1,
        ADMIN_ID : {
          $first: '$TASK_ASSIGNED_TO.id'
        },
        EXPORT_COUNTRIES: showImports ? '$IMPORT_COUNTRIES' : '$EXPORT_COUNTRIES',
        EXPORTER_COUNTRY:showImports ? '$BUYER_COUNTRY' : '$EXPORTER_COUNTRY',
        FOLDER_NAME:1
      }
    }
    if(FOB_BY_HS){
      projectStage["$project"]["FOB_BY_HS"] = FOB_BY_HS
    }
    let contactFilter = {}
    if(AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length){
      let newObj= []
      for(let i=0;i<=AVAILABLE_CONTACTS.length - 1 ; i++){
        const element = AVAILABLE_CONTACTS[i]
        if(element.alt === 'contact_count'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
              {"EXTRA_DETAILS.Email ID" : {$exists:false}}
            ]
          })
        }else if(element.alt === 'email_count'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
              {"EXTRA_DETAILS.Email ID" : {$exists:true}}
            ]
          })
        }else if(element.alt === 'both_count'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:true}},
              {"EXTRA_DETAILS.Email ID" : {$exists:true}}
            ]
          })
        }else if(element.alt === 'both_not'){
          newObj.push( {
            $and : [
              {"EXTRA_DETAILS.Contact Number" : {$exists:false}},
              {"EXTRA_DETAILS.Email ID" : {$exists:false}}
            ]
          })
        }
      }
      contactFilter = {
        $or : newObj
      }
    }
    let  turnoverFilter = {}
    if(TURNOVER_RANGE && TURNOVER_RANGE.length >= 1){
      let newObj = []
      for(let i = 0; i<= TURNOVER_RANGE.length - 1;i++){
        const element = TURNOVER_RANGE[i]
        if(element.minVal !== undefined && element.maxVal !== undefined){
          newObj.push({
            [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB'] : {
              $gte:element.minVal,
              $lte:element.maxVal
            }
          })
        }else{
          newObj.push({
            [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB']:{
              $gte:element.maxVal
            }
          })
        }
      }
      turnoverFilter = {
        $or : newObj
      }
    }
    let statusFilter = {}
    if(STATUS && STATUS.length){
      let newObj=[]
      if(areArraysOfObjectsIdentical(statusArr,STATUS,"name")){
        newObj.push(
          {"$or": [ {
            "ADMIN_ID": {
              "$ne": null
            }
          }]}
        )
      }else if(isArraySubsetOfAnother(statusArr,STATUS,"name")){
        newObj.push(
          {"$or": [ {
            "ADMIN_ID": {
              "$ne": null
            }
          }]}
        )
        for(let i = 0; i<= STATUS.length - 1;i++){
          const element = STATUS[i]
          if(!isStringInArrayOfObjects(statusArr,element.name)){
            if(element.status != undefined || element.status != null){
           
              if(element.status === 0){
               newObj.push({
                 'STATUS' : {"$ne": null}
               })
              }else if(element.status === 'Pending'){
               newObj.push({
                 $and: [
                   {'STATUS' : 0},
                   {
                     "$or": [ {
                       "ADMIN_ID": {
                         "$ne": null
                       }
                     }]
                   }
                 ]
               })
              }
              else{
               newObj.push({
                 'STATUS': element.status
                })
              }
             }else if(element.name === 'Not assigned'){
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$eq": null
                   }
                 }]} 
               )
             }else{
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$ne": null
                   }
                 }]}
               )
             }
          }
      
        }
      }else{
        for(let i = 0; i<= STATUS.length - 1;i++){
          const element = STATUS[i]
          if(element.status != undefined || element.status != null){
           
              if(element.status === 0){
               newObj.push({
                $and: [
                  {'STATUS' : {"$ne": null}},
                  {
                    "$or": [ {
                      "ADMIN_ID": {
                        "$ne": null
                      }
                    }]
                  }
                ]
                 
               })
              }else if(element.status === 'Pending'){
               newObj.push({
                 $and: [
                   {'STATUS' : 0},
                   {
                     "$or": [ {
                       "ADMIN_ID": {
                         "$ne": null
                       }
                     }]
                   }
                 ]
               })
              }
              else{
               newObj.push({
                $and: [
                  {'STATUS': element.status},
                  {
                    "$or": [ {
                      "ADMIN_ID": {
                        "$ne": null
                      }
                    }]
                  }
                ]
                 
                })
              }
             }else if(element.name === 'Not assigned'){
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$eq": null
                   }
                 }]} 
               )
             }else{
               newObj.push(
                 {"$or": [ {
                   "ADMIN_ID": {
                     "$ne": null
                   }
                 }]}
               )
             }
          
      
        }
      }
      
      
      statusFilter = {
        $or : newObj
      }
      
    }

    const matchConditions = [
      searchParam ? {
        $or: [
          { ['EXPORTER_NAME']: { $regex: new RegExp(`${searchParam}`, 'i') } },
          { "HS_CODES.HS_CODES": { $regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`) } }
        ]
      } : {},
      country_name ? { ['EXPORTER_COUNTRY']: country_name } : {},
      EXPORTER_NAMES && EXPORTER_NAMES.length  ? {['EXPORTER_NAME']: {$in: EXPORTER_NAMES}} :{},
      search ? { 
        $or: [
          {['EXPORTER_NAME']: {$regex: new RegExp(search) , $options:'i'}},
          { 'EXTRA_DETAILS.Contact Number': {$regex: new RegExp(search),$options:'i'}}
        ] 
      } : {},
      BUYERS && BUYERS.length ? { ['BUYERS']: { $in: BUYERS } } : {},
      COUNTRIES && COUNTRIES.length ? { ['EXPORT_COUNTRIES']: { $in: COUNTRIES } } : {},
      ORGANIZATION_TYPE && ORGANIZATION_TYPE.length ? organiztionType : {},
      CITIES && CITIES.length ? { ['EXPORTER_CITY']: { $in: CITIES } } : {},
      leadAssignedTo && leadAssignedTo.length ? { 'TASK_ASSIGNED_TO.contact_person': { $in: leadAssignedTo } } : {},
      contactNo && contactNo.length ? {
        $or: [
          { 'EXTRA_DETAILS.Contact Number': { $in: contactNo } },
          { 'EXTRA_DETAILS.Contact Number': { $in: contactNo.map(item => item.toString()) } }
        ]
      } : {},
      contactPerson && contactPerson.length ? { 'EXTRA_DETAILS.Contact Person': { $in: contactPerson } } : {},
      designation && designation.length ? { 'EXTRA_DETAILS.Designation': { $in: designation } } : {},
      TURNOVER_RANGE && TURNOVER_RANGE.length ? turnoverFilter : {},
      STATUS && STATUS.length ? statusFilter : {},
      AVAILABLE_CONTACTS && AVAILABLE_CONTACTS.length? contactFilter : {}
    ];
  
    const matchStage = {
      $match: {
        $and :matchConditions
      }
    };
    pipelinedata.push(matchStage);
    const totalCountPipeline = [projectStage,...pipelinedata,{$count: "dbCount" }];
    const dataPipeline = [projectStage,...pipelinedata];
    
    // dataPipeline.push({
    //   $sort:{
    //     [FOB_BY_HS? 'FOB_BY_HS' :'FOB']:-1
    //   }
    // })
    if(sortBuyerCount){
      dataPipeline.push({
        $sort:{
          'TOTAL_BUYERS': sortBuyerCount
        }
      })
    }else if(sortCity){
      dataPipeline.push({
        $sort:{
          'EXPORTER_CITY': sortCity
        }
      })
    }else if(sortCompanyName){
      dataPipeline.push({
        $sort:{
          'EXPORTER_NAME': sortCompanyName
        }
      })
    }else if(sortContactPerson){
      dataPipeline.push({
        $sort:{
          'EXTRA_DETAILS.Contact Person': sortContactPerson
        }
      })
    }else if(sortTurnover){
      dataPipeline.push({
        $sort:{
          [FOB_BY_HS ? 'FOB_BY_HS' :  'FOB']: sortTurnover
        }
      })
    }else if(sortleadAssigned){
      dataPipeline.push({
        $sort:{
          'TASK_ASSIGNED_TO.contact_person': sortleadAssigned
        }
      })
    }
    else if(FOB_BY_HS){
      dataPipeline.push({
        $sort:{
          'FOB_BY_HS': -1
        }
      })
    }else{
      dataPipeline.push({
        $sort:{
          'FOB': -1
        }
      })
    }
    const paginationStages = [
      { $skip: (currentPage - 1) * resultPerPage },
      { $limit: parseInt(resultPerPage)  }
    ];
  
    if (currentPage && resultPerPage) {
      dataPipeline.push(...paginationStages);
    }
    const countPromise = showImports ? BuyerModelV2.aggregate(totalCountPipeline).exec() : ExporterModelV2.aggregate(totalCountPipeline).exec();
    const dataPromise =  showImports ? BuyerModelV2.aggregate(dataPipeline).exec() : ExporterModelV2.aggregate(dataPipeline).exec();
    const [countResult, dataResult]  = await Promise.all([countPromise, dataPromise])
    const totalCount = countResult[0] ? countResult[0].dbCount : 0;
        
    res.send({ 
      success:true,
      message:{
        message: dataResult,
        total_records: totalCount
      }
    }); 
    // .then(([countResult, dataResult]) => {
    //     const totalCount = countResult[0] ? countResult[0].dbCount : 0;
        
    //     res.send({ 
    //       success:true,
    //       message:{
    //         message: dataResult,
    //         total_records: totalCount
    //       }
    //     });
    //   })
    //   .catch(error => {
    //     console.log('Error in crm master',error);
    //     throw new Error(`Error retrieving CRM master data: ${error.message}`);
    //   });
  }catch(e){
    console.log('Error in data',e);
    res.send(e)
  }
}


exports.getCRMMasterTblFiltersV2 = async (req,res) => {
  try{
    const result = await getCRMMasterTblFiltersV2Func(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCRMMasterTblFiltersV2Func = async ({country_name,searchParam,HS_CODES,onlyShowForUserId,BUYERS,COUNTRIES,EXPORTER_CODES,showImports,EXPORTER_NAMES}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let pipelinedata = []
      let searchObj = {}
      const reddisVariable = `${country_name}-${searchParam}-${HS_CODES.join(",")}-${onlyShowForUserId ? onlyShowForUserId : ''}-${BUYERS?.join(",") || ''}-${COUNTRIES?.join(",") || ""}-${showImports ? 'Buyers' :'Exporters'}`
      const cachedData = await redisInstance.redisGetSync(reddisVariable)
      if(cachedData){
        return resolve({
          success:true,
          message:JSON.parse(cachedData)
        })
      }
      const matchConditions = [
        searchParam ? {
          $or: [
            { [showImports ? 'BUYER_NAME' :'EXPORTER_NAME']: { $regex: new RegExp(`${searchParam}`, 'i') } },
            { "HS_CODES.HS_CODES": { $regex: new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`) } }
          ]
        } : {},
        EXPORTER_NAMES && EXPORTER_NAMES.length ? {[showImports ? 'BUYER_NAME' :'EXPORTER_NAME']: {$in: EXPORTER_NAMES}} :{},
        country_name ? { [showImports ? 'BUYER_COUNTRY' :'EXPORTER_COUNTRY']: country_name } : {},
        BUYERS && BUYERS.length ? { [showImports ? 'EXPORTERS' : 'BUYERS']: { $in: BUYERS } } : {},
        COUNTRIES && COUNTRIES.length ? { [showImports ? 'IMPORT_COUNTRIES' :'EXPORT_COUNTRIES']: { $in: COUNTRIES } } : {},
        HS_CODES && HS_CODES.length ? { 'HS_CODES.HS_CODES': {$in:  HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`))}} : {}
      ];
    
      const matchStage = {
        $match: {
          $and : matchConditions
        }
      };  
      pipelinedata = [matchStage]

      pipelinedata.push({
        $project: {
          'EXPORTER_NAME': showImports ? '$BUYER_NAME' : '$EXPORTER_NAME',
          'EXPORTER_CITY': showImports ? '$BUYER_CITY' : '$EXPORTER_CITY',
          'EXTRA_DETAILS': 1       
        }
      })

      const otherpipeline = [...pipelinedata]
      otherpipeline.push({
        $unwind : "$EXTRA_DETAILS"
      })
      otherpipeline.push( {
        $group: {
          _id: null,
          'Contact_Person': {
            $addToSet : {
              name:'$EXTRA_DETAILS.Contact Person'
            }
          },
          'Contact_Number': {
            $addToSet : {
              name:'$EXTRA_DETAILS.Contact Number'
            }
          },
           'Designation': {
            $addToSet : {
              name:'$EXTRA_DETAILS.Designation'
            }
          }
        }
      })
     
     
      pipelinedata.push({
        $group : {
          '_id': null,
          'EXPORTER_CITY': {
            '$addToSet': {
              name:  '$EXPORTER_CITY'
            }
          },
          'EXPORTER_NAME':{
            '$addToSet': {
              'name' : '$EXPORTER_NAME'
            }
          }
        }
      })
      const p1 = showImports ? BuyerModelV2.aggregate(pipelinedata)   : ExporterModelV2.aggregate(pipelinedata)
      const p2 = showImports ? BuyerModelV2.aggregate(otherpipeline)  : ExporterModelV2.aggregate(otherpipeline)
      const [response,response2] = await Promise.all([p1,p2])
  
      let filterData = {}
      filterData["Company Name"] = {
        "accordianId": 'companyName',
        type: "checkbox",
        labelName: "name",
        data : response?.[0]?.EXPORTER_NAME
      }

  
      filterData["Contact No"] = {
        "accordianId": 'contactNo',
        type: "checkbox",
        labelName: "name",
        data: response2?.[0]?.Contact_Number
      }

      filterData["Contact Person"] = {
        "accordianId": 'contactPerson',
        type: "checkbox",
        labelName: "name",
        data: response2?.[0]?.Contact_Person
      }
  
      filterData["Designation"] = {
        "accordianId": 'designation',
        type: "checkbox",
        labelName: "name",
        data: response2?.[0]?.Designation
      }

      filterData["Exporter City"] =  {
        "accordianId": 'CITIES',
        type: "checkbox",
        labelName: "name",
        data:response?.[0]?.EXPORTER_CITY
      }
      if(!onlyShowForUserId){
        filterData["Lead Assigned To"] = {
          "accordianId": 'leadAssignedTo',
          type: "checkbox",
          labelName: "name"
        }
        let query = `SELECT tbl_user_details.contact_person AS name FROM tbl_user 
        LEFT JOIN tbl_user_details ON tbl_user.id = tbl_user_details.tbl_user_id WHERE tbl_user.isSubUser = 1 AND tbl_user.type_id = 1 `
        let dbRes = await call({ query }, 'makeQuery', 'get');
        filterData["Lead Assigned To"]["data"] = dbRes.message
      }
      await redisInstance.redisSetSync(reddisVariable,JSON.stringify(filterData))
      resolve({
        success:true,
        message : filterData
      })
    }catch(e){
      console.log('error in e',e);
      reject({
        success:false,
        message:'Failed to fetch records'
      })
    }
  })
}

exports.getCRMMasterdataFiltersV2 = async (req,res) => {
  try{
    const result = await getCRMMasterdataFiltersV2Func(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCRMMasterdataFiltersV2Func = async ({ country_name, searchParam, HS_CODES, EXPORTER_CODES,showImports,EXPORTER_NAMES }) => {
 return new Promise(async(resolve,reject) => {
  try {
    const pipelinedata = [];
    const reddisVariable = `${country_name}${searchParam}${HS_CODES.join(",")}`
    const cachedData = await redisInstance.redisGetSync(reddisVariable)
    if(cachedData){
      return resolve({
        success: true,
        message: JSON.parse(cachedData)
      })
    }
    const searchRegex = new RegExp(`^${searchParam.length === 1 ? "0" + searchParam : searchParam}`);
    const searchObj = searchParam ? isNaN(parseInt(searchParam))
      ? { [showImports ? 'BUYER_NAME' : 'EXPORTER_NAME']: { $regex: new RegExp(`${searchParam}`, 'i') } }
      : { "HS_CODES.HS_CODES": { $regex: searchRegex } } : {};

    const ttvSearchObj = { ...searchObj, [showImports ? 'CONSIGNEE_NAME' : 'EXPORTER_NAME']: { $regex: new RegExp(`${searchParam}`, 'i') } };
    const hsObj = HS_CODES && HS_CODES.length
      ? { "HS_CODES.HS_CODES": { $in: HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`)) } }
      : {};

    const ttvHSObj = HS_CODES && HS_CODES.length
      ? { "HS_CODE": { $in: HS_CODES.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`)) } }
      : {};

    pipelinedata.push({
      $match: {
        $and: [
          searchObj,
          { [showImports ? 'BUYER_COUNTRY' : 'EXPORTER_COUNTRY'] : country_name },
          EXPORTER_NAMES  && EXPORTER_NAMES.length ? { [showImports ? 'BUYER_NAME' : 'EXPORTER_NAME'] : {$in: EXPORTER_NAMES}} :{},
          hsObj
        ]
      }
    });

    const response = showImports ? await BuyerModelV2.aggregate([
      ...pipelinedata,
      {
        $group: {
          '_id': null,
          'EXPORTER_CITY': { '$addToSet':'$BUYER_CITY' },
          'EXPORTER_COUNT': { '$sum': 1 }
        }
      }
    ]) : await ExporterModelV2.aggregate([
      ...pipelinedata,
      {
        $group: {
          '_id': null,
          'EXPORTER_CITY': { '$addToSet': '$EXPORTER_CITY' },
          'EXPORTER_COUNT': { '$sum': 1 }
        }
      }
    ]);


    pipelinedata.push({
      $project: {
        'EXPORTER_NAME': showImports ? '$BUYER_NAME' :'$EXPORTER_NAME' ,
        'EXPORTER_CITY': showImports ? '$BUYER_CITY' :'$EXPORTER_CITY' ,
        'EXTRA_DETAILS': { $ifNull: ['$EXTRA_DETAILS', []] },
        'EXPORTER_CODE':showImports ? '$BUYER_CODE' :'$EXPORTER_CODE' ,
        'EXPORT_COUNTRIES' : showImports ? '$IMPORT_COUNTRIES' :'$EXPORT_COUNTRIES' ,
        'BUYERS' : showImports ? '$EXPORTERS' :'$BUYERS'
      }
    });

    const countPipeline = (matchObj) => [
      ...pipelinedata,
      { $match: matchObj },
      { $count: 'count' }
    ];
    let totalContacts = 0
    let totalEmails = 0
    let totalBoth = 0
    let noneboth = 0

    let uniqueExportersBoth = new Set();
    let uniqueExportersContacts = new Set();
    let uniqueExportersEmails = new Set();
    let uniqueExportersNone = new Set();
    let expCodes = []
    let expCountries = []
    let buyers = []
    const exporters = showImports ? await BuyerModelV2.aggregate(pipelinedata) : await ExporterModelV2.aggregate(pipelinedata)
    for(let i=0; i<= exporters.length - 1 ; i++){
      const element = exporters[i]
      expCodes.push(element.EXPORTER_CODE)
      expCountries = [
        ...expCountries,
        ...element["EXPORT_COUNTRIES"] ,
      ]
      buyers = [
        ...buyers,
        ...element["BUYERS"],
      ]
      let expContacts = false;
      let expEmails = false
      let expBoth = false
      let expNone = false
      if(element?.EXTRA_DETAILS && element?.EXTRA_DETAILS.length >= 1){
        for(let j=0; j<= element?.EXTRA_DETAILS?.length - 1 ; j++){
          let item = element.EXTRA_DETAILS[j]
          if(item["Contact Number"] && item["Email ID"]){
            totalBoth += 1
            expBoth = true
          }else if(item["Contact Number"] && !item["Email ID"]){
            totalContacts += 1
            expContacts = true
          }else if(item["Email ID"] && !item["Contact Number"]){
            totalEmails += 1
            expEmails = true
          }else{
            noneboth += 1
            expNone = true 
          }
        }
      }else{
        noneboth += 1
        expNone = true 
      }
     
      if (expBoth) {
        uniqueExportersBoth.add(element.EXPORTER_NAME);
      }else if(expContacts){
        uniqueExportersContacts.add(element.EXPORTER_NAME);
      }else if(expEmails){
        uniqueExportersEmails.add(element.EXPORTER_NAME);
      }else if (expNone){
        uniqueExportersNone.add(element.EXPORTER_NAME);
      } 
    }
    let ttvExpCodes = {
      [showImports ? 'BUYERS': 'EXPORTERS'] : {
        $in : expCodes
      }
    }

    const buyerspipeline = [
      { $match : ttvExpCodes},
      {
        $group: {
          _id: showImports ? '$EXPORTER_NAME' : '$BUYER_NAME',
          BUYER_NAME: { $first: showImports ? '$EXPORTER_NAME' : '$BUYER_NAME' },
          BUYER_CODE: { $first: showImports ? '$EXPORTER_CODE' : '$BUYER_CODE'}
        }
      },
      { $sort: { [showImports ? 'EXPORTER_NAME' : 'BUYER_NAME']: 1 } },
      { $project: { _id: 0 } }
    ];

    const countriespipeline = [
      { $match : ttvExpCodes},
      { $match: { $and: [ttvSearchObj, ttvHSObj] } },
      {
        $group: {
          _id: '$DESTINATION_COUNTRY',
          DESTINATION_COUNTRY: { $first: '$DESTINATION_COUNTRY' }
        }
      },
      { $sort: { DESTINATION_COUNTRY: 1 } },
      { $project: { _id: 0 } }
    ];
    console.log('buyers',buyers);
    //const buyersResponse = showImports ? await ExporterModelV2.aggregate(buyerspipeline) : await BuyerModelV2.aggregate(buyerspipeline)
    // const [buyersResponse, countriesResponse] = await Promise.all([
    //   TTVSummaryV2.aggregate(buyerspipeline),
    //   TTVSummaryV2.aggregate(countriespipeline)
    // ]);
    const finalRes = {
      ...response?.[0],
      email_count: uniqueExportersEmails.size || 0,
      contact_count: uniqueExportersContacts.size || 0,
      both_count: uniqueExportersBoth.size || 0,
      both_not: uniqueExportersNone.size || 0,
      EXPORT_COUNTRIES: Array.from(new Set(expCountries)),
      BUYER_NAMES: Array.from(new Set(buyers)),
      totalContacts,
      noneboth,
      totalBoth,
      totalEmails
    }
    await redisInstance.redisSetSync(reddisVariable,JSON.stringify(finalRes))
    return resolve({
      success: true,
      message: finalRes
    });
  } catch (e) {
    console.log('error in e', e);
    resolve( {
      success: false,
      message: 'Failed to fetch records'
    });
  }
 })
};


exports.getBuyerListCRMV2 = async (req,res) => {
  try{
    const result = await getBuyerListCRMV2Func(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getBuyerListCRMV2Func = async ({buyers,resultPerPage,currentPage,search,EXPORTER_NAME,EXPORTER_COUNTRY}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let searchObj = {}
      if(search){
        searchObj = {
          'CONSIGNEE_NAME': {$regex: new RegExp(search), $options:'i'}
        }
      }
      // let buyerdata = []
      // let exporterdata = await ExporterModelV2.find({EXPORTER_NAME:EXPORTER_NAME,EXPORTER_COUNTRY:EXPORTER_COUNTRY})
      // buyerdata = exporterdata[0].BUYERS
      const mainpipeline = [
        {
          '$match': {
            '$and': [ 
              {
                'EXPORTER_NAME': EXPORTER_NAME
              },
              searchObj
            ]
          }
        }, {
          '$group': {
            '_id': '$CONSIGNEE_NAME', 
            'TOTAL_SHIPMENTS': {
              '$sum': 1
            }, 
            'DESTINATION_COUNTRY': {
              '$first': '$DESTINATION_COUNTRY'
            }, 
            'FOB': {
              '$sum': '$FOB_VALUE_USD'
            }, 
            'HSN_CODES': {
              '$addToSet': '$HS_CODE'
            }, 
            'PRODUCT_TYPE': {
              '$addToSet': '$PRODUCT_TYPE'
            },
            'CONSIGNEE_NAME':{$first:"$CONSIGNEE_NAME"}
          }
        }, {
          '$sort': {
            'FOB': -1
          }
        }
      ]
      const countPipeline = [...mainpipeline]
      countPipeline.push({
        $count:'total_records'
      })
      
      if(currentPage && resultPerPage) {
        mainpipeline.push({
          '$skip': (currentPage - 1) * parseInt(resultPerPage) 
        })
        mainpipeline.push({
          '$limit': parseInt(resultPerPage) 
        })
      }
      const response = await TTV.aggregate(mainpipeline)
      const countRes = await TTV.aggregate(countPipeline)
      resolve({
        success:true,
        message:{
          message:response,
          total_records: countRes?.[0]?.total_records || 0
        }
      })
    }catch(e){
      console.log('error in buyers API',e);
      reject({
        success:false,
        message:''
      })
    }
  })
  
}


exports.getHSNListCRMV2 = async (req,res) => {
  try{
    const result = await getHSNListCRMV2Func(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getHSNListCRMV2Func = async ({resultPerPage,currentPage,search,EXPORTER_NAME,EXPORTER_COUNTRY}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let searchObj = {}
      if(search){
        searchObj = {
          'HS_CODE': {$regex: new RegExp("^"+search), $options:'i'}
        }
      }
      const mainpipeline = [
        {
          '$match': {
            'EXPORTER_NAME': EXPORTER_NAME,
            ...searchObj
          }
        }, {
          '$group': {
            '_id': {
              'HS_CODE': {
                '$substr': [
                  '$HS_CODE', 0, 2
                ]
              }, 
              'DESTINATION_COUNTRY': '$DESTINATION_COUNTRY'
            }, 
            'TOTAL_SHIPMENTS': {
              '$sum': 1
            }, 
            'SUB_CODES': {
              '$addToSet': '$HS_CODE'
            }, 
            'BUYERS': {
              '$addToSet': '$CONSIGNEE_NAME'
            }, 
            'FOB': {
              '$sum': '$FOB_VALUE_USD'
            }, 
            'HS_TWO_DIGIT': {
              '$first': {
                '$substr': [
                  '$HS_CODE', 0, 2
                ]
              }
            }
          }
        }, {
          '$sort': {
            'FOB': -1
          }
        }, {
          '$group': {
            '_id': '$_id.HS_CODE', 
            'TOTAL_SHIPMENTS': {
              '$sum': '$TOTAL_SHIPMENTS'
            }, 
            'FOB': {
              '$sum': '$FOB'
            }, 
            'TOP_COUNTRIES': {
              '$push': {
                'DESTINATION_COUNTRY': '$_id.DESTINATION_COUNTRY', 
                'FOB_BY_COUNTRY': '$FOB'
              }
            }, 
            'BUYERS': {
              '$addToSet': '$BUYERS'
            }, 
            'SUB_CODES': {
              '$addToSet': '$SUB_CODES'
            }
          }
        }, {
          '$project': {
            '_id': 1, 
            'TOTAL_SHIPMENTS': 1, 
            'FOB': 1, 
            'TOP_COUNTRIES': 1, 
            'BUYERS': {
              '$reduce': {
                'input': '$BUYERS', 
                'initialValue': [], 
                'in': {
                  '$setUnion': [
                    '$$value', '$$this'
                  ]
                }
              }
            }, 
            'SUB_CODES': {
              '$reduce': {
                'input': '$SUB_CODES', 
                'initialValue': [], 
                'in': {
                  '$setUnion': [
                    '$$value', '$$this'
                  ]
                }
              }
            }
          }
        }, {
          '$lookup': {
            'from': 'tbl_hsn_mapping', 
            'localField': '_id', 
            'foreignField': 'HS_CODE', 
            'as': 'hsn_master'
          }
        }, {
          '$project': {
            '_id': 0, 
            'HS_CODE': '$_id', 
            'PRODUCT_DESCRIPTION': {
              '$first': '$hsn_master.Description'
            }, 
            'TOTAL_SHIPMENTS': 1, 
            'SUB_CODES': {
              '$size': '$SUB_CODES'
            }, 
            'BUYERS': {
              '$size': '$BUYERS'
            }, 
            'FOB': 1, 
            'TOP_COUNTRIES': {
              '$slice': [
                '$TOP_COUNTRIES', 3
              ]
            }
          }
        },
        {
          $sort: {
            'FOB':-1
          }
        }
      ]
      const countPipeline = [...mainpipeline]
      countPipeline.push({
        $count:'total_records'
      })
      
      if(currentPage && resultPerPage) {
        mainpipeline.push({
          '$skip': (currentPage - 1) * parseInt(resultPerPage) 
        })
        mainpipeline.push({
          '$limit': parseInt(resultPerPage) 
        })
      }
      const response = await TTV.aggregate(mainpipeline)
      const countRes = await TTV.aggregate(countPipeline)
      resolve({
        success:true,
        message:{
          message:response,
          total_records: countRes?.[0]?.total_records || 0
        }
      })
    }catch(e){
      console.log('error in buyers API',e);
      reject({
        success:false,
        message:''
      })
    }
  })
}

function modifyCompanyString(inputString) {
  // Remove any dots "."
  let modifiedString = inputString?.replace(/\./g, "");
  
  // Replace "Private Limited" with "PVT LTD"
  modifiedString = modifiedString?.replace(/Private Limited/g, "PVT LTD");
  
  // Replace "Limited" with "LTD"
  modifiedString = modifiedString?.replace(/Limited/g, "LTD");

  return modifiedString?.toUpperCase();
}

exports.uploadMasterExcel = async (req,res) => {
  try{
    const result = await uploadMasterExcelFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const uploadMasterExcelFunc = ({ showImports }, reqFiles) => {``
  return new Promise(async(resolve, reject) => {
    try {
      let filepath = ''
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        let shwImp = showImports == 'false' ? false : true
        console.log('sheet namesssss',shwImp);
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
        let result = [];
        let emailIds = []
        let isSearchByEmail = false
        for (let i = 0; i <= data.length - 1; i++) {
          let obj = data[i]
          if(obj["Email ID"]){
            emailIds.push(obj["Email ID"])
            isSearchByEmail = true
          }else{
            result.push(modifyCompanyString(shwImp ? obj.BUYER_NAME : obj.EXPORTER_NAME))
          }
          
        }
        
        if(isSearchByEmail){
          const resp = await ExporterModelV2.aggregate([
            {
              $match: {'EXTRA_DETAILS.Email ID': {$in: emailIds}}
            },
            {
              $project:{
                _id:0,
                EXPORTER_NAME:1
              }
            }
          ])
          console.log('Exporters',resp,JSON.stringify([
            {
              $match: {'EXTRA_DETAILS.Email ID': {$in: emailIds}}
            },
            {
              $project:{
                _id:0,
                EXPORTER_NAME:1
              }
            }
          ]))
          result = resp.map(item => item.EXPORTER_NAME)
        }

        resolve({
          success:true,
          message: result
        })
      }

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}

exports.addAdminRemark = async (req,res) => {
  try{
    let {userId, remark, invApplicationId, lcApplicationId} = req.body
    let query = invApplicationId ? formatSqlQuery(`INSERT INTO tbl_admin_remarks (userId,remark,invApplicationId) VALUES (?,?,?)`, [
      userId,remark,invApplicationId
    ]) : formatSqlQuery(`INSERT INTO tbl_admin_remarks (userId,remark,lcApplicationId) VALUES (?,?,?)`, [
      userId,remark,lcApplicationId
    ])
    await dbPool.query(query)
    res.send({
      success: true,
    })    
  }catch(e){
    res.send({
      success: false,
    })
  }
}

exports.getAdminRemarks = async (req,res) => {
  try{
    let {invApplicationId, lcApplicationId, showUpto3} = req.body
    let query = invApplicationId ? formatSqlQuery(`SELECT tbl_admin_remarks.createdAt, tbl_admin_remarks.remark,
    tbl_user_details.contact_person
    FROM tbl_admin_remarks LEFT JOIN tbl_user_details ON
    tbl_admin_remarks.userId = tbl_user_details.tbl_user_id  WHERE tbl_admin_remarks.invApplicationId = ? 
    ORDER BY tbl_admin_remarks.createdAt DESC ${showUpto3 ? ' LIMIT 3 ' : ' ' } `, [
      invApplicationId
    ]) : formatSqlQuery(`SELECT tbl_admin_remarks.createdAt, tbl_admin_remarks.remark,
    tbl_user_details.contact_person
    FROM tbl_admin_remarks LEFT JOIN tbl_user_details ON
    tbl_admin_remarks.userId = tbl_user_details.tbl_user_id  WHERE tbl_admin_remarks.lcApplicationId = ? 
    ORDER BY tbl_admin_remarks.createdAt DESC ${showUpto3 ? ' LIMIT 3 ' : ' ' } `, [
      lcApplicationId
    ])
    let dbRes = await call({ query }, 'makeQuery', 'get');
    res.send({
      success: true,
      message: dbRes.message
    })    
  }catch(e){
    res.send({
      success: false,
    })
  }
}


exports.updateCorporatePricing = async (req,res) => {
  try{
    let reqBody = req.body
    console.log('reqBoodyyyyyy',reqBody);
    const updateQuery = await ExporterModelV2.findOneAndUpdate({_id: new ObjectId(reqBody._id)},{$set: {PRICING : reqBody.pricing}})
    console.log('updateQuery',updateQuery);
    res.send({
      success:true,
      message:'Pricing Updated'
    })
  }catch(e){
    console.log('error in updateCorporatePricing',e)
    res.send({
      success:false,
      message:'Failed to update Price.'
    })
  }
}

exports.addCRMExpComment = async (req,res) => {
  try{
    let reqBody = req.body
    await ExporterComments.create({
      _id:new ObjectId(),
      EXPORTER_CODE:reqBody.EXPORTER_CODE,
      EXPORTER_NAME:reqBody.EXPORTER_NAME,
      CREATED_BY : reqBody.SUBADMIN_NAME,
      CREATED_USER_ID: reqBody.SUB_ADMIN_USER_ID,
      CREATED_AT: new Date(),
      COMMENT: reqBody.COMMENT
    })
    res.send({
      success:true,
      message:'Comment Added Succesfully'
    })
  }catch(e){
    console.log('error in addCRMExpComment',e)
    res.send({
      success:false,
      message:'Failed to add Comment'
    })
  }
}

exports.getExpComment = async (req,res) => {
  try{
    let reqBody = req.body
    const result = await ExporterComments.find({EXPORTER_CODE: reqBody.EXPORTER_CODE}).sort({CREATED_AT: -1})
    res.send({
      success:true,
      message:result
    })
  }catch(e){
    console.log('error in getExpComment',e)
    res.send({
      success:false,
      message:e
    })
  }
}

exports.sendEmailFromCRM = async (req,res) => {
  try{
    let reqBody = req.body
    let reqFiles = req.files
    let docArr = ''
    let attachmentArr = []
    if(reqFiles){
      let filesInfo = [];
      for (const [key, data] of Object.entries(reqFiles)) {
        filename =data.name
        fs.writeFileSync('./docs/' + data.md5, data.data);
        filepath = path.resolve(__dirname, `../../docs/${data.md5}`)
        attachmentArr.push({   
          filename: filename,
          path: filepath 
        })
        let fileDataInfo = {
          doc_no: "",
          doc_name: key,
          file_name: data.name,
          gen_doc_label: 'emailTemplateDocs',
          file_hash: data.md5,
          valid_upto: null,
          category: 19,
          mst_doc_id: 60,
          created_at: new Date(),
          created_by: reqBody.userId,
          modified_at: new Date()
        }
        filesInfo.push(fileDataInfo);
      }
      
      const docInsertObj = {
        tableName: 'tbl_document_details',
        insertArr: filesInfo
      }

      let response = await call(docInsertObj, 'setMultipleData', 'POST');
      if (!response.success) {
        console.log('Error while inserting swift doc:', dbResObj.message)
        throw errors.databaseApiError;
      }

      let docIDArr = [];
      for(let i = 0; i < response.message.length; i ++){
        let docID = response.message[i].dataValues.id;
        docIDArr.push(docID);
      }
      docArr = docIDArr.join(',');
    
  }
    let htmlBody = `<p><b>${reqBody.subject}</b><br>${reqBody.mailBody}</p>`
    let mailOptions = {
      from: config.mail.user, // sender address
      to: JSON.parse(reqBody.emailIds), // list of receivers
      bcc: bccEmails,
      subject: reqBody.subject, // Subject line
      html: reqBody.mailBody  
    }

    if(attachmentArr.length){
      mailOptions ["attachments"] = attachmentArr
    }
    sendMail(mailOptions,null,reqBody.userId)
    if(reqBody.type === 'TRF Admin'){
      const sql = `INSERT INTO tbl_user_tasks_logs (EXPORTER_CODE,EXPORTER_NAME,EVENT_TYPE,CREATED_BY,LOG_TYPE,REMARK,DOCS) VALUES ('${reqBody.EXPORTER_CODE}', '${reqBody.EXPORTER_NAME}','EMail','${reqBody.userId}', 'Email Sent','${mysqlTextParse(htmlBody)}','${docArr}')`;
      console.log('Queryyyy',sql,htmlBody)
      await dbPool.query(sql)

    }else if(reqBody.type === 'TRF CRM'){
      await CRMTasksLogs.create({
        EXPORTER_CODE:reqBody.EXPORTER_CODE,
        EXPORTER_NAME:reqBody.EXPORTER_NAME,
        EVENT_TYPE:'EMail',
        REMARK: htmlBody,
        LOG_TYPE:'Email Sent',
        ADMIN_ID:reqBody.userId,
        ADMIN_NAME:reqBody.userName,
        DOCS:docArr
      })
    }else if(reqBody.type === 'TRF Admin Enquiry'){
      const sql = `INSERT INTO tbl_enquiry_tasks_logs (EXPORTER_CODE,EXPORTER_NAME,EVENT_TYPE,CREATED_BY,LOG_TYPE,REMARK,DOCS) VALUES ('${reqBody.EXPORTER_CODE}', '${reqBody.EXPORTER_NAME}','EMail','${reqBody.userId}', 'Email Sent','${mysqlTextParse(htmlBody)}','${docArr}')`;
      console.log('Queryyyy',sql,htmlBody)
      await dbPool.query(sql)

    }
    res.send({
      success:true,
      message:'Mail Sent Succesfully'
    })
  }catch(e){
    console.log('error in sendEmailFromCRM',e)
    res.send({
      success:false,
      message:e
    })
  }
}

exports.addEmailTemplates = async (req,res) => {
  try{
    let {userId,mailBody,subject,adminId} = req.body
    let reqFiles = req.files
    let docArr = ''
    let filename = ''
    let filepath =''
    if(reqFiles){
        let filesInfo = [];
        for (const [key, data] of Object.entries(reqFiles)) {
          filename =data.name
          fs.writeFileSync('./docs/' + data.md5, data.data);
          filepath = path.resolve(__dirname, `../../docs/${data.md5}`)

          let fileDataInfo = {
            doc_no: "",
            doc_name: key,
            file_name: data.name,
            gen_doc_label: 'emailTemplateDocs',
            file_hash: data.md5,
            valid_upto: null,
            category: 19,
            mst_doc_id: 60,
            created_at: new Date(),
            created_by: adminId,
            modified_at: new Date()
          }
          filesInfo.push(fileDataInfo);
        }
        
        const docInsertObj = {
          tableName: 'tbl_document_details',
          insertArr: filesInfo
        }

        let response = await call(docInsertObj, 'setMultipleData', 'POST');
        if (!response.success) {
          console.log('Error while inserting swift doc:', dbResObj.message)
          throw errors.databaseApiError;
        }

        let docIDArr = [];
        for(let i = 0; i < response.message.length; i ++){
          let docID = response.message[i].dataValues.id;
          docIDArr.push(docID);
        }
        docArr = docIDArr.join(',');
      
    }
    let query = `INSERT INTO tbl_crm_mail_templates( tbl_user_id, email_subject, email_body,docs ,created_by, modified_by) VALUES ('${adminId}', '${mysqlTextParse(subject)}' , '${mysqlTextParse(mailBody)}','${docArr ? docArr : ''}','${userId}','${userId}')`
    await dbPool.query(query)
    res.send({
      success:true,
      message:'Added Email to templates'
    })
  }catch(e){
    console.log('error in addEmailTemplates',e)
    res.send({
      success:false,
      message:e
    })
  }
}

exports.getEmailTemplates = async (req,res) => {
  try{
    let {adminId} = req.body
    let query = `SELECT * FROM tbl_crm_mail_templates WHERE tbl_user_id=${adminId}`
    const dbRes = await call({query},'makeQuery','get')
    if(dbRes.message.length === 0){
      return res.send({
        success:false,
        message:'No Saved Templates Found'
      })
    }
    res.send({
      success:true,
      message:dbRes.message
    })
  }catch(e){
    console.log('error in addEmailTemplates',e)
    res.send({
      success:false,
      message:'Failed to load templates'
    })
  }
}

exports.getAllIndiaExporters = async (req, res) => {
  try {
    let pipeline = []
    // check if already exist on platform start
    let query = formatSqlQuery(`SELECT DISTINCT tbl_user_details.*, tbl_user.ttvExporterCode FROM tbl_user_details
    LEFT JOIN tbl_user ON tbl_user.id = tbl_user_details.tbl_user_id
    WHERE tbl_user_details.company_name LIKE ? AND tbl_user.type_id = ?
     `, [`%${req.body.supplierName}%`, 19])
    if(req.body.userTypeId/1 == 20){
      if(req.body.supplierName){
        query = formatSqlQuery(`SELECT DISTINCT tbl_user_details.*, tbl_user.ttvExporterCode FROM tbl_user_details
    LEFT JOIN tbl_user ON tbl_user.id = tbl_user_details.tbl_user_id
    LEFT JOIN tbl_network_requests ON tbl_user_details.tbl_user_id = tbl_network_requests.request_to 
    WHERE tbl_user_details.company_name LIKE ? AND tbl_user.type_id = ? AND tbl_network_requests.request_from = ?
     `, [`%${req.body.supplierName}%`, 19, req.body.userId])
      }
      else{
        query = formatSqlQuery(`SELECT DISTINCT tbl_user_details.*, tbl_user.ttvExporterCode FROM tbl_user_details
    LEFT JOIN tbl_user ON tbl_user.id = tbl_user_details.tbl_user_id
    LEFT JOIN tbl_network_requests ON tbl_user_details.tbl_user_id = tbl_network_requests.request_to 
    WHERE tbl_user.type_id = ? AND tbl_network_requests.request_from = ?
     `, [ 19, req.body.userId])
      }
    }
    let dbRes = await call({ query }, 'makeQuery', 'get')
    // check if already exist on platform end
    if (dbRes.message.length) {
      let userData = []
      let seen = new Set()
      for (let index = 0; index < dbRes.message.length; index++) {
        const element = dbRes.message[index];
        if (!seen.has(element.company_name)) {
          seen.add(element.company_name);
        userData.push({
          'EXPORTER_ADDRESS': element.user_address,
          'EXPORTER_CODE': element.ttvExporterCode,
          'EXPORTER_NAME': element.company_name,
          'industryType': element.industry_type,
          'EXTRA_DETAILS': [{
            'Contact Number': element.contact_number,
            'Contact Person': element.contact_person,
            'Department': element.designation,
            'Email ID': element.email_id,
            'Designation':element.designation,
          }]
        })
      
      }
    }
    console.log("exporter length",dbRes.message.length,userData.length)

      res.send({
        success: true,
        message: userData
      })
    }
    else if(req.body.userTypeId/1 != 20 && req.body.supplierName) {
      pipeline.push({
          $match: {
            'EXPORTER_NAME': {
              $regex: new RegExp(req.body.supplierName),
              $options: 'i'
            }
          }
      })      
      pipeline.push({
        '$project': {
          '_id': 0,
          'EXPORTER_NAME': 1,
          'EXPORTER_CODE': 1,
          'EXTRA_DETAILS': 1,
          'EXPORTER_ADDRESS': 1
        }
      })
      const response = await ExporterModelV2.aggregate(pipeline)
    console.log("exporter length 2",response?.length,response)

      res.send({
        success: response.length != 0,
        message: response
      })
    }
    else{
      res.send({
        success: false,
        message: []
      })
    }
  } catch (e) {
    res.send({
      success: false,
      message: []
    })
  }
}

exports.createdirectapplication = async (req, res) => {
  console.log(req.body ,"--->>> req body")
  try {
    const { cpUserId, supplierName, EXPORTER_CODE, name_title, phone_code, contactPerson, contact_number, designation, email_id, industry_type, organization_type, user_address,
      gstDocument, iecDocument, panDocument, cinDocument} = req.body
    let exp_code = EXPORTER_CODE
    // Check if user alredy exists start
    let checkUserQuery = formatSqlQuery(`SELECT * FROM tbl_user WHERE login_id = ? `, [email_id])
    let userDbResp = await call({ query: checkUserQuery }, 'makeQuery', 'get')
    if (userDbResp.message.length) {
      if (cpUserId) {
        addUserInNetworkFunc({
          loggedUserId: cpUserId,
          networkUserArray: [{ "id": userDbResp.message[0]["id"] }],
          status: 5
        })
      }
      let returnResp = await LoginV2({body: {username: userDbResp.message[0]["login_id"], password: userDbResp.message[0]["password"], bypassAccountNotActiveError: true}})
      console.log("returnResppppppppppppppp",returnResp);
      res.send({
        success: true,
        message: returnResp.message
      })
    }
    // Check if user alredy exists end
    else {
      if (!EXPORTER_CODE) {
        let exp_code = Math.floor(100000 + Math.random() * 900000)
        await ExporterModelV2.create({
          EXPORTER_NAME: supplierName,
          EXPORTER_CODE: exp_code,
          EXTRA_DETAILS: [{
            "Contact Number": contact_number,
            "Contact Person": contactPerson,
            "Email ID": email_id,
          }],
          EXPORTER_ADDRESS: user_address

        })
      }
      const userRes = await ExporterModelV2.find({ EXPORTER_CODE: EXPORTER_CODE })
      if (userRes?.[0]?.tbl_user_id && userRes?.[0]?.STATUS === 4) {
        const query = `SELECT
        tbl_user.type_id,
        tbl_user.login_id AS email,
        tbl_user.id AS user_id,
        tbl_user_details.company_name AS userName,
        tbl_user_details.contact_person AS main_user_name
      FROM
        tbl_user
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id WHERE tbl_user.id = '${userRes?.[0]?.tbl_user_id}'`
        'ttvExporterCode'
        const dbRes = await call({ query }, 'makeQuery', 'get')
        let resobj = dbRes.message?.[0] || {}
        resobj = {
          ...resobj,
          ttvExporterCode: EXPORTER_CODE
        }
        return res.send({
          success: true,
          message: resobj
        })
      }
      //insert in tbl_user
      const query = `INSERT INTO tbl_user ( login_id, user_name, password, tech_type_id, type_id,domain_key ,parent_id, status, step, created_by, modified_by, ttvExporterCode,created_at) VALUES 
                                        ('${email_id}', '${email_id}', 'U2FsdGVkX18zoSwd1vU3GYuTgfCBoMqKKJXid1MqCS0=', '2', '19' ,'19', 0, 0, '1', 0, 0, '${exp_code}', '${getCurrentTimeStamp()}')`
      const dbRes = await dbPool.query(query)
      const tbl_user_id = dbRes[0].insertId

      //insert in tbl_user_details
      const query2 = `INSERT INTO tbl_user_details ( identifier, tbl_user_id, public_key, rsa_public_key, company_name, designation, address, user_address, kyc_done, iec_no, gst_vat_no, email_id, contact_person, name_title, contact_number, phone_code, country_code, reference_no, created_at, created_by, modified_at, modified_by, bc_usr_reg_flag, bc_usr_reg_time, bc_usr_enrl_flag, bc_user_enrl_time, bc_usr_reg_ledger_flag, bc_usr_reg_ledger_time, has_plan, plan_id, plan_quota, user_avatar, company_pan_verification, company_pan_result, company_gst_verification, company_gst_result, company_iec_verification, company_iec_result, company_cin_verification, company_cin_result, credit_status, credit_show_to, credit_created_at, credit_modified_at, cin_no, pan_no, ifsc_no, license_no, organization_type, industry_type, stenn_user_id, stenn_kyc, finance_type, channel_partner_role, aadhar_no, company_aadhar_verification, company_aadhar_result, company_city, company_postal_code, company_address1, company_address2, company_state, company_country, bio, financialSummary, UserPermissions, iecDetailsSyncedTill) VALUES 
                ( 'USER${new Date().getTime()}' , '${tbl_user_id}', NULL, NULL, '${supplierName}', '${designation}', NULL, '${mysqlTextParse(user_address)}' , '0', '${iecDocument}', '${gstDocument}', '${email_id}', '${contactPerson}', '${name_title}', '${contact_number}', '${phone_code}', 'IN', NULL, '${getCurrentTimeStamp()}', '${tbl_user_id}', '${getCurrentTimeStamp()}', '${tbl_user_id}', '0', '${getCurrentTimeStamp()}', '0', '${getCurrentTimeStamp()}', '0', '${getCurrentTimeStamp()}', '0', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '${cinDocument}', '${panDocument}', NULL, NULL, '${organization_type}', '${industry_type}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', 'null', NULL, NULL, '', NULL, NULL, NULL, NULL, NULL)`

      await dbPool.query(query2)

      //insert in tbl_user_details_extra
      const query5 = `INSERT INTO tbl_user_details_extra (tbl_user_id,company_email,business_address,created_by,modified_by) VALUES
                    ('${tbl_user_id}', '${email_id}','${mysqlTextParse(user_address)}','${tbl_user_id}','${tbl_user_id}')`

      await dbPool.query(query5)

      //activate free plan
      const query3 = `INSERT INTO tbl_subscriptions_balance (tbl_user_id, plan_id, lc_nos, buyer_nos, transactions_nos, points_nos, invoice_nos, createdBy, createdAt, updatedBy, updatedAt) VALUES 
                              ('${tbl_user_id}', '5', '1', '2', '3', '0', '5', '${tbl_user_id}', '${getCurrentTimeStamp()}', '${tbl_user_id}', '${getCurrentTimeStamp()}')`

      await dbPool.query(query3)

      //skip tutorial
      const query4 = `INSERT INTO tbl_tutorial_status ( userId, status, skippedStage,createdAt, updatedAt) VALUES 
    ( '${tbl_user_id}', '1', '11', '${getCurrentTimeStamp()}', '${getCurrentTimeStamp()}')`
      await dbPool.query(query4)
      fetchUserBuyerDetailsFromTTVData(tbl_user_id)
      //return the required result 
      let response = {
        type_id: 19,
        email: email_id,
        user_id: tbl_user_id,
        userName: supplierName,
        ttvExporterCode: exp_code,
        main_user_name: contactPerson
      }
      if (cpUserId) {
        addUserInNetworkFunc({
          loggedUserId: cpUserId,
          networkUserArray: [{ "id": tbl_user_id }],
          status: 5
        })
      }
      res.send({
        success: true,
        message: response
      })
    }
  } catch (e) {
    console.log('error in create new application', e);
    res.send({
      success: false,
      message: []
    })
  }
}

exports.saveAdminMailCredentials = async (req, res) => {
  try {
    let { mail, account, password } = req.body
    let tempMailConfig = {}
    if (account === "gmail") {
      tempMailConfig["service"] = config.mail.service
    }
    else if (account === "hostinger") {
      tempMailConfig = {
        host: "smtp.hostinger.com",
        port: 465
      }
    }
    else if (account === "outlook") {
      tempMailConfig = {
        host: "smtp-mail.outlook.com",
        port: 587
      }
    }
    tempMailConfig["auth"] = {
      user: mail,
      pass: password
    }
    // console.log("tempMailConfiggggggggggggg",tempMailConfig);
    let transporter = nodemailer.createTransport(tempMailConfig)
    let mailOption = { to: [mail], subject: 'Self Test Mail', html: 'Self Test Mail' }
    transporter.sendMail(mailOption, async function (err, info) {
      // console.log("sendmailrespppppp", err, info);
      if (err) {
        res.send({
          success: false
        })
      }
      else {
        await dbPool.query(formatSqlQuery(`DELETE FROM tbl_admin_mail_credentials WHERE mail = ? `, [mail]))
        await dbPool.query(formatSqlQuery(`INSERT INTO tbl_admin_mail_credentials (mail,account,password) VALUES (?,?,?) `, [mail, account, encryptData(password)]))
        res.send({
          success: true
        })
      }
    });
  }
  catch (e) {
    console.log('errorinsaveAdminMailCredentials', e);
    res.send({
      success: false
    })
  }
}


exports.getAdminHistoryTasks = async (req,res) => {
  try{
    const result = await getAdminHistoryTasksFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getAdminHistoryTasksFunc = ({ currentPage ,resultPerPage, dateRangeFilter,taskUpdate,search,onlyShowForUserId,included_status,leadAssignedTo,hscodes,leadsStatus,requirements,taskStatus,TasksState,taskType,contactNo,subadminIds }) => {
  return new Promise(async(resolve, reject) => {
    try {
    let matchobj  = {}
    if(dateRangeFilter && dateRangeFilter.length >=1){
      if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
        matchobj = {
          $expr: {
            $eq: [
              { $substr: ['$CREATED_AT', 0, 10] }, // extract the first 10 characters (date component)
                dateRangeFilter?.[0]  // compare with the target date string
            ]
          }
        }
           
      }else{
        matchobj = {
          'CREATED_AT' :{
            $gte: new Date(dateRangeFilter?.[0]),
            $lte: new Date(dateRangeFilter?.[1])
           }
        }
      }
    }
    
    let includedTasks = []
    if(taskUpdate?.includes("User Onboarded")){
      if(taskUpdate && taskUpdate.length == 1){
        includedTasks = [4]
      }else{
        includedTasks.push(4)
      }
    }
    let mainPipeline = [
      { 
        $match : matchobj
      },  
      {
        $group:{
          _id: "$EXPORTER_CODE",
          LOG_TYPE: {
            $last: "$LOG_TYPE",
          },
          EXPORTER_NAME: {
            $last: "$EXPORTER_NAME",
          },
          LastEventTime: {
            $last: "$CREATED_AT",
          },
          EXPORTER_CODE: {
            $last: "$EXPORTER_CODE",
          },
          LastNote : {
            $last : '$REMARK'
          },
          EVENT_TYPE : {
            $last : '$EVENT_TYPE'
          },
          EVENT_STATUS : {
            $last : '$EVENT_STATUS'
          },
          ADMIN_NAME:{
            $last:'$ADMIN_NAME'
          }
        },
    },
    {
      $lookup:
        {
          from:  env === 'dev' ? "india_export_exporters_list_prod" : "india_export_exporters_list_prod",
          localField: "EXPORTER_CODE",
          foreignField: "EXPORTER_CODE",
          as: "crm_tasks",
        },
    },
    {
      $addFields: {
        firstMatch: {
          $arrayElemAt: ["$crm_tasks", 0],
        },
      },
    },
    {
      $replaceRoot: {
        newRoot: {
          $mergeObjects: ["$firstMatch", "$$ROOT"]
        },
      },
    },
    {
      $project: {
        crm_tasks: 0,
        firstMatch: 0, 
      },
    }]
    mainPipeline.push({
      $match :{
        $and: [
          {TASK_TYPE: taskType},
          {STATUS : {$in:[0,1,2,3,4]}},
          { "TASK_ASSIGNED_TO.id" : {$exists : true}}
        ]
      }
    })
    if(onlyShowForUserId){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.id":onlyShowForUserId
        }
      })
    }
    if(subadminIds && subadminIds.length){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.id":{
            $in: subadminIds
          }
        }
      })
    }
    let FOB_BY_HS = null
    if(hscodes && hscodes.length){
      const hsCodesRegex = hscodes.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
      mainPipeline.push({
        $match: {
          "HS_CODES.HS_CODES": { $in: hsCodesRegex }
        }
      });
      FOB_BY_HS = {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$HS_CODES",
                as: "code",
                cond: {
                  $in: [
                    { $substr: [ "$$code.HS_CODES", 0, 2 ] },
                    hscodes
                  ]
                }
              }
            },
            as: "code",
            in: "$$code.FOB_VALUE_USD"
          }
        }
      } 
    }
    if(requirements && requirements.length){
      mainPipeline.push({
        $match: {
          'INTRESTED_SERVICES' : {$in : requirements}
        }
      })
      
    }

    if(leadAssignedTo && leadAssignedTo.length){
      mainPipeline.push({
        $match: {
          "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
        }
      })
    }
    if(search){
      mainPipeline.push({
        $match:{
          $or: [
            {EXPORTER_NAME: {$regex: new RegExp(search) , $options:'i'}},
            { 'EXTRA_DETAILS.Contact Number': {$regex: new RegExp(search),$options:'i'}}
          ]
          
        }
      })
    }
 
   
    mainPipeline.push({
      $lookup: {
        from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
        localField: 'EXPORTER_CODE',
        foreignField: 'EXPORTER_CODE',
        as: 'task_logs'
      }
    })
    mainPipeline.push({
      $addFields :{
        "task_logs": {
          "$ifNull": ["$task_logs", []]
        }
      }
    })
    let projectObj = {
      EXPORTER_ADDRESS:1,
      EXPORTER_CITY:1,
      EXPORTER_CODE:1,
      EXPORTER_NAME:1,
      EXTRA_DETAILS:1,
      FOB:1,
      STATUS:1,
      TASK_ASSIGNED_TO:1,
      TOP_COUNTRIES:1,
      TOTAL_BUYERS:1,
      LastNote: 1,
      LastEventTime: 1,
      LastEventType : 1,
      LAST_NOTE:1,
      LOG_TYPE: 1,
      HS_CODES:1,
      TOTAL_SHIPMENTS:1,
      EVENT_STATUS:1,
      TASK_DATE:1,
      task_logs:1,
      EXPORTER_COUNTRY:1,
      PRICING:1,
      ADMIN_NAME:1
    }
    
    if(FOB_BY_HS){
      projectObj["FOB"] = FOB_BY_HS
    }
    mainPipeline.push({
      $project : projectObj
    })
    if(TasksState && TasksState.length){
      if(TasksState.includes('Task Created') && TasksState.includes('Task Not Created')){
        // mainPipeline.push({
        //   $match: {
        //     'LastNote' : 
        //   }
        // })
      }else if(TasksState.includes('Task Created')){
        mainPipeline.push({
          $match: {
            'LOG_TYPE' : {
              $exists: true
            }
          }
        })
      }else if(TasksState.includes('Task Not Created')){
        mainPipeline.push({
          $match: {
            'LOG_TYPE' : {
              $exists: false
            }
          }
        })
      }
    }
    if(contactNo && contactNo.length){
      if(contactNo.includes('Number Available') && contactNo.includes('Number Not Available')){
       
      }else if(contactNo.includes('Number Available')){
        mainPipeline.push({
          $match: {
            'EXTRA_DETAILS.Contact Number' : {
              $exists: true
            }
          }
        })
      }else if(contactNo.includes('Number Not Available')){
        mainPipeline.push({
          $match: {
            'EXTRA_DETAILS.Contact Number' : {
              $exists: false
            }
          }
        })
      }
    }
    
    // mainPipeline.push({
    //   $sort : {
    //     'TASK_DATE': 1,
    //   } 
    // })
    if(taskStatus && taskStatus.length){
      mainPipeline.push({
        $match: {
          'EVENT_STATUS' : {
            $in : taskStatus.map(item => new RegExp(item))
          }
        }
      })
    } 
    if(leadsStatus && leadsStatus.length){
      if(leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': [0,1,2,3,4]
            }
          }
        })
      }else if(leadsStatus.includes("Lead Created")){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': [1]
            }
          }
        })
      }else if(leadsStatus.includes("Lead Not Created")){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': [0,2,3,4]
            }
          }
        })
      }
    }
    if(taskUpdate){
      let statusArray = taskUpdate.filter(element => element !== 'User Onboarded' && element !== 'Lead Created')
      if(statusArray && statusArray.length ){
        mainPipeline.push({
          $match:{
            $or : [
              {
                'STATUS': {
                  '$in': includedTasks
                }
              },
              {$and : [
                {'LOG_TYPE' : 'Didnt connect'},
                {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
              ]
              },
              {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
            ]
          }
        })
      }else{
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': includedTasks
            }
          }
        })
      }
        // mainPipeline.push({
        //   $match:{
        //     $or : [
        //       {
        //         'STATUS': {
        //           '$in': includedTasks
        //         }
        //       },
        //       statusArray && statusArray.length ? {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }} : {}
        //     ]
        //   }
        // })
      
    }else{
      if(!leadsStatus){
        mainPipeline.push({
          $match:{
            'STATUS': {
              '$in': included_status
            }
          }
        })
      }
    }
    
    const countpipeline = [...mainPipeline]
    let countoptimized = [...countpipeline]
    // if(!(taskStatus || taskUpdate || TasksState)){
    //   countoptimized = countpipeline.filter((stage) => !("$lookup" in stage))
    // }
    countoptimized.push({
      '$count': 'total_records'
    })
    const countRes = await CRMTasksLogs.aggregate(countoptimized)
    const total_count = countRes[0]?.total_records
    if(currentPage && resultPerPage) {
      mainPipeline.push({
        '$skip': (currentPage - 1) * parseInt(resultPerPage) 
      })
      mainPipeline.push({
        '$limit': parseInt(resultPerPage) 
      })
    }  
    
    // mainPipeline.push({
    //   $lookup:{
    //     from: 'tbl_exporters_lists',
    //     localField: 'EXPORTER_CODE',
    //     foreignField: 'EXPORTER_CODE',
    //     as: 'exporter_data'

    //   }})
      mainPipeline.push({
        $project : {
          EXPORTER_ADDRESS:1,
          EXPORTER_CITY:1,
          EXPORTER_CODE:1,
          EXPORTER_NAME:1,
          EXTRA_DETAILS:1,
          FOB:1,
          STATUS:1,
          TASK_ASSIGNED_TO:1,
          TOP_COUNTRIES:1,
          TOTAL_BUYERS:1,
          LastNote: 1,
          LastEventTime: 1,
          LastEventType : 1,
          LAST_NOTE:1,
          LOG_TYPE: 1,
          HS_CODES:1,
          TOTAL_SHIPMENTS:1,
          EVENT_STATUS:1,
          BUYERS: 1,
          export_data:1,
          DidntConnectCount: {
            "$size": {
              "$filter": {
                "input": "$task_logs",
                "cond": {
                  "$eq": ["$$this.LOG_TYPE", "Didnt connect"]
                }
              }
            }
          },
          EXPORTER_COUNTRY:1,
          PRICING:1,
          ADMIN_NAME:1
        }
      })
    if(taskType === 'Corporate'){
      mainPipeline.push({
        $lookup: {
          from: env === 'dev' ? 'tbl_crm_applications' : 'tbl_crm_applications_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'crm_applications'
        }
      })
    }

      const response = await CRMTasksLogs.aggregate(mainPipeline)
      resolve({
        success:true,
        message:{
          message:response,
          total_count
        }
      })

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}


exports.getCallListStatsV2 = async(req,res) => {
  try{
    const result = await getCallListStatsV2Func(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCallListStatsV2Func = ({taskUpdate,search,dateRangeFilter,onlyShowForUserId,leadAssignedTo,hscodes,leadsStatus,requirements,taskStatus,included_status,taskType}) =>{
  return new Promise(async(resolve,reject)=> {
    try{
      let matchobj  = {}
      let tasksmatchObj ={ }
      if(dateRangeFilter && dateRangeFilter.length >=1){
        if( dateRangeFilter?.[0] == dateRangeFilter?.[1] ){
          matchobj = {
            $expr: {
              $eq: [
                { $substr: ['$TASK_DATE', 0, 10] }, // extract the first 10 characters (date component)
                  dateRangeFilter?.[0]  // compare with the target date string
              ]
            }
          }
          tasksmatchObj = {
            $expr: {
              $eq: [
                { $substr: ['$CREATED_AT', 0, 10] }, // extract the first 10 characters (date component)
                  dateRangeFilter?.[0]  // compare with the target date string
              ]
            }
          }
             
        }else{
          matchobj = {
            'TASK_DATE' :{
              $gte: new Date(dateRangeFilter?.[0]),
              $lte: new Date(dateRangeFilter?.[1])
             }
          }
          tasksmatchObj = {
            'CREATED_AT' :{
              $gte: new Date(dateRangeFilter?.[0]),
              $lte: new Date(dateRangeFilter?.[1])
             }
          }
        }
      }
      let mainPipeline = [
      { 
        $match : matchobj
      },
      {
        $match : {
          'TASK_ASSIGNED_TO' : {$exists : true},
          "TASK_TYPE": "Call List"
        }
      }
      ]
      let tasksPipeline = [
        { 
          $match : tasksmatchObj
        }
      ]
      if(taskType === 'Exporter Wise'){
        tasksPipeline.push({
          $group:{
            _id: "$EXPORTER_CODE",
            LOG_TYPE: {
              $last: "$LOG_TYPE",
            },
            EXPORTER_NAME: {
              $last: "$EXPORTER_NAME",
            },
            CREATED_AT: {
              $last: "$CREATED_AT",
            },
            EVENT_TIME : {
              $last : '$EVENT_TIME'
            },
            EXPORTER_CODE: {
              $last: "$EXPORTER_CODE",
            },
            REMARK : {
              $last : '$REMARK'
            },
            EVENT_TYPE : {
              $last : '$EVENT_TYPE'
            },
            EVENT_STATUS : {
              $last : '$EVENT_STATUS'
            }
          },
        })
      }
      tasksPipeline.push({
        $lookup : {
          from: env === 'dev' ? "india_export_exporters_list" : "india_export_exporters_list_prod",
          localField: "EXPORTER_CODE",
          foreignField: "EXPORTER_CODE",
          as: "crm_tasks",
        }    
      })
      tasksPipeline.push({
        $project : {
          TASK_ASSIGNED_TO : {
            $first : '$crm_tasks.TASK_ASSIGNED_TO'
          },
          EXPORTER_NAME  : {
            $first : '$crm_tasks.EXPORTER_NAME'
          },
          HS_CODES: {
            $first : '$crm_tasks.HS_CODES'
          },
          STATUS : {
            $first : '$crm_tasks.STATUS'
          },
          LOG_TYPE : 1,
          EVENT_TIME: 1,
          EVENT_TYPE:1,
          INTRESTED_SERVICES : {
            $first : '$INTRESTED_SERVICES'
          }
        }
      })
      if(onlyShowForUserId){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.id":onlyShowForUserId
          }
        })
        tasksPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.id":onlyShowForUserId
          }
        })
      }
      if(leadAssignedTo && leadAssignedTo.length){
        mainPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
          }
        })
        tasksPipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.contact_person": {$in : leadAssignedTo}
          }
        })
      }

      let includedTasks = []
      if(taskUpdate?.includes("User Onboarded")){
        if(taskUpdate && taskUpdate.length == 1){
          includedTasks = [4]
        }else{
          includedTasks.push(4)
        }
      }
      if(hscodes && hscodes.length){
        const hsCodesRegex = hscodes.map(item => new RegExp(`^${item.length === 1 ? "0" + item : item}`));
        mainPipeline.push({
          $match: {
            "HS_CODES.HS_CODES": { $in: hsCodesRegex }
          }
        });
        tasksPipeline.push({
          $match: {
            "HS_CODES.HS_CODES": { $in: hsCodesRegex }
          }
        });
      }
      if(requirements && requirements.length){
        mainPipeline.push({
          $match: {
            'INTRESTED_SERVICES' : {$in : requirements}
          }
        })
        tasksPipeline.push({
          $match: {
            'INTRESTED_SERVICES' : {$in : requirements}
          }
        })
      }
  
    
      if(search){
        mainPipeline.push({
          $match:{
            EXPORTER_NAME: {$regex: new RegExp(search) , $options:'i'}
          }
        })
        tasksPipeline.push({
          $match:{
            EXPORTER_NAME: {$regex: new RegExp(search) , $options:'i'}
          }
        })
      }
      mainPipeline.push({
        $lookup : {
          from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'task_logs'
        }
      })
      let  pendingPipeline =  mainPipeline
      pendingPipeline = [...pendingPipeline]
      if(taskType === 'Exporter Wise'){
        pendingPipeline.push({
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            },
            STATUS:1,
            TASK_ASSIGNED_TO:1,
            LAST_NOTE:1,
            LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
            EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
            TASK_DATE:1,
            EVENT_TIME:{$last: '$task_logs.EVENT_TIME'},
            EXPORTER_CODE:1
          }
        })
      }
      
      mainPipeline.push(
        taskType === 'Exporter Wise'? {
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            },
            STATUS:1,
            TASK_ASSIGNED_TO:1,
            LAST_NOTE:1,
            LOG_TYPE: {$last: '$task_logs.LOG_TYPE'},
            EVENT_STATUS:{$last: '$task_logs.EVENT_STATUS'},
            TASK_DATE:1,
            EVENT_TIME:{$last: '$task_logs.EVENT_TIME'},
            EXPORTER_CODE:1
          }
        } :{
          '$unwind': {
              'path': '$task_logs', 
              'includeArrayIndex': 'i', 
              'preserveNullAndEmptyArrays': true
          }
        },
      )
      if(taskType === 'Task Wise'){
        mainPipeline.push({
          $project: {
            EVENT_STATUS : '$task_logs.EVENT_STATUS',
            STATUS : 1,
            LOG_TYPE:'$task_logs.LOG_TYPE',
            TASK_DATE:1 ,
            EVENT_TIME:'$task_logs.EVENT_TIME',
            EXPORTER_CODE:1
          }
        })
      }
      if(taskStatus && taskStatus.length){
        mainPipeline.push({
          $match: {
            'EVENT_STATUS' : {
              $in : taskStatus.map(item => new RegExp(item))
            }
          }
        })
        tasksPipeline.push({
          $match: {
            'EVENT_STATUS' : {
              $in : taskStatus.map(item => new RegExp(item))
            }
          }
        })
        if(taskType === 'Exporter Wise'){
          pendingPipeline.push({
            $match: {
              'EVENT_STATUS' : {
                $in : taskStatus.map(item => new RegExp(item))
              }
            }
          })
        }else{
          pendingPipeline.push({
            $match: {
              '$task_logs.EVENT_STATUS' : {
                $in : taskStatus.map(item => new RegExp(item))
              }
            }
          })
        }
      } 
      if(leadsStatus && leadsStatus.length){
        if(leadsStatus.includes("Lead Created") && leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
          tasksPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,1,2,3,4]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
          tasksPipeline.push({
            $match:{
              'STATUS': {
                '$in': [1]
              }
            }
          })
        }else if(leadsStatus.includes("Lead Not Created")){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
          tasksPipeline.push({
            $match:{
              'STATUS': {
                '$in': [0,2,3,4]
              }
            }
          })
        }
      }
      if(taskUpdate){
        let statusArray = taskUpdate.filter(element => element !== 'User Onboarded' && element !== 'Lead Created')
        if(statusArray && statusArray.length ){
          mainPipeline.push({
            $match:{
              $or : [
                {
                  'STATUS': {
                    '$in': includedTasks
                  }
                },
                {$and : [
                  {'LOG_TYPE' : 'Didnt connect'},
                  {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
                },
                {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
              ]
            }
          })
          tasksPipeline.push({
            $match:{
              $or : [
                {
                  'STATUS': {
                    '$in': includedTasks
                  }
                },
                {$and : [
                  {'LOG_TYPE' : 'Didnt connect'},
                  {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
                },
                {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
              ]
            }
          })
          if(taskType === 'Exporter Wise'){
            pendingPipeline.push({
              $match:{
                $or : [
                  {
                    'STATUS': {
                      '$in': includedTasks
                    }
                  },
                  {$and : [
                    {'LOG_TYPE' : 'Didnt connect'},
                    {'EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                  ]
                  },
                  {LOG_TYPE: {$in: statusArray.map(item => new RegExp(item)) }}
                ]
              }
            })
          }else{
            pendingPipeline.push({
              $match:{
                $or : [
                  {
                    'STATUS': {
                      '$in': includedTasks
                    }
                  },
                  {$and : [
                    {'$task_logs.LOG_TYPE' : 'Didnt connect'},
                    {'$task_logs.EVENT_STATUS': {$in: statusArray.map(item => new RegExp(item)) }}
                  ]
                  },
                  {'$task_logs.LOG_TYPE': {$in: statusArray.map(item => new RegExp(item)) }}
                ]
              }
            })
          }
        }else{
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
          tasksPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': includedTasks
              }
            }
          })
        }  
      }else{
        if(!leadsStatus){
          mainPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
          pendingPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
          tasksPipeline.push({
            $match:{
              'STATUS': {
                '$in': included_status
              }
            }
          })
        }
      }
  
      const tasksOverallPipeline =  [ ...mainPipeline,   
       taskType === 'Exporter Wise'? {
        '$group': {
          '_id': null, 
          'tasksFollowup': {
            '$sum': {
              '$cond': [
                {
                  '$in': [
                    '$LOG_TYPE', [
                      'Call back','Create New Task'
                    ]
                  ]
                }, 1, 0
              ]
            }
          }, 
          'tasksNew': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    { '$type': '$LOG_TYPE' },
                    'missing'
                  ]
                }, 1, 0
              ]
            }
          }
        }
      } : {
        '$group': {
          '_id': null, 
          'tasksFollowup': {
            '$sum': {
              '$cond': [
                {
                  '$in': [
                    '$LOG_TYPE', [
                      'Call back', 'Create New Task'
                    ]
                  ]
                }, 1, 0
              ]
            }
          }, 
          'tasksNew': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    { '$type': '$LOG_TYPE' },
                    'missing'
                  ]
                }, 1, 0
              ]
            }
          }
        }
      }
      ]

      const logTypePipeline = [...tasksPipeline]
      
      logTypePipeline.push({
        $group : {
          _id: '$LOG_TYPE',
          'total_records' : {$sum: 1},
          'LOG_TYPE':{$last:'$LOG_TYPE'}
        }
      })
      mainPipeline.push({
        $group : {
          _id: '$EVENT_STATUS',
          'total_records' : {$sum: 1},
          'EVENT_TYPE':{$first : '$EVENT_STATUS'}
        }
      })
      let tasksInComplete = 0
      let tasksCompleted = 0
      const eventResponse = await ExporterModelV2.aggregate(mainPipeline)
      const logsResponse = await CRMTasksLogs.aggregate(logTypePipeline)
      const pendingResponse = await ExporterModelV2.aggregate(pendingPipeline)
      const tasksOverallResponse  = await ExporterModelV2.aggregate(tasksOverallPipeline)
      for(let i=0; i<= pendingResponse.length - 1 ; i++){
        const element = pendingResponse[i]
        if(taskType === 'Exporter Wise'){
          if(element.LOG_TYPE === undefined){
            tasksInComplete += 1
          }else{
            const TasksLogs = element.task_logs
            if(TasksLogs.LOG_TYPE === 'Lead Lost' || TasksLogs.LOG_TYPE === 'User Onboarded' ){
              tasksCompleted += 1
            }else if(TasksLogs.LOG_TYPE === 'Didnt connect' && TasksLogs.EVENT_TIME === undefined){
              tasksCompleted +=1
            }
            else if((TasksLogs.LOG_TYPE === 'Call back' || TasksLogs.LOG_TYPE  === 'Create New Task' || TasksLogs.LOG_TYPE === 'Not Interested' || TasksLogs.LOG_TYPE === 'Didnt connect') && (new Date(TasksLogs.EVENT_TIME).getTime() >= new Date(dateRangeFilter[0]).getTime() && (new Date(TasksLogs.EVENT_TIME).getTime() <= new Date(dateRangeFilter[1]).getTime()))){
              tasksCompleted += 1
            }else {
              tasksInComplete += 1
            }
          }
        }
      }
      if(taskType === 'Task Wise'){
        tasksInComplete = tasksOverallResponse?.[0]?.tasksNew
        logsResponse.forEach(item => {
           tasksCompleted += item.total_records
        })
      }
      resolve({
        success:true,
        message:{
          eventResponse,
          logsResponse,
          leadsCount : logsResponse?.filter(item => item.LOG_TYPE === 'Lead Created')?.[0]?.total_records,
          onboardCount :logsResponse?.filter(item => item.LOG_TYPE === 'User Onboarded')?.[0]?.total_records,
          pendingCount :tasksInComplete,
          completedCount:tasksCompleted,
          newTaskCount : tasksOverallResponse?.[0]?.tasksNew,
          FollowupCount : tasksOverallResponse?.[0]?.tasksFollowup,
        }
      })
    }catch(e){
      console.log('error in apio',e);
      reject({
        success:false
      })
    }
  })
}

exports.excelSummary = async (req,res) => {
  try{
    const result = await excelSummaryFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const excelSummaryFunc = ({ showImports }, reqFiles) => {``
  return new Promise(async(resolve, reject) => {
    try {
      let filepath = ''
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        let shwImp = showImports == 'false' ? false : true
        console.log('sheet namesssss',shwImp);
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[1]]);
                const result = [];
        let currentObj = {};
        let not_added = []
        for (let i = 0; i <= data.length - 1; i++) {
          let obj = data[i]
          if (obj) {
            if (obj["EXPORTER_CODE"]) {
              if (Object.keys(currentObj).length) {
                result.push(currentObj);
              }
              const newObj = {
                EXPORTER_CODE: obj.EXPORTER_CODE,
                EXPORTER_NAME: obj.EXPORTER_NAME,
                EXPORTER_ADDRESS: obj.EXPORTER_ADDRESS,
                EXPORTER_CITY: obj.EXPORTER_CITY,
                TOTAL_BUYERS: obj.TOTAL_BUYERS,
                // FOB: obj.FOB,
                // FOB_IN_MILLION: obj['FOB (in million $)'],
                HS_CODE: obj['HS Code'],
                //TASK_DATE : taskDate
              }
              newObj["EXTRA_DETAILS"] = [{
                'Department': obj.Department,
                'GST/ Establishment Number': obj['GST/ Establishment Number'],
                'Contact Number': obj['Contact Number'],
                'DIN': obj['DIN'],
                'Contact Number': obj['Contact Number'],
                'Email ID': obj['Email ID']
              }]
              currentObj = { ...newObj };
              delete obj['Department']
              delete obj['GST/ Establishment Number']
              delete obj['Contact Number']
              delete obj['DIN']
              delete obj['Contact Number']
              delete obj['Email ID']
            } else if(obj["EXPORTER_NAME"]){
              const exporterinfo =await ExporterModel.find({EXPORTER_NAME : {
                $regex : new RegExp(obj.EXPORTER_NAME),
                $options: 'i'
              }})
              let expObj = exporterinfo?.[0]
              if(expObj){
                if (Object.keys(currentObj).length) {
                  result.push(currentObj);
                }
                const newObj = {
                  EXPORTER_CODE: expObj.EXPORTER_CODE,
                  EXPORTER_NAME: expObj.EXPORTER_NAME,
                  EXPORTER_ADDRESS: expObj.EXPORTER_ADDRESS,
                  EXPORTER_CITY: expObj.EXPORTER_CITY,
                  TOTAL_BUYERS: expObj.TOTAL_BUYERS,
                  // FOB: obj.FOB,
                  // FOB_IN_MILLION: obj['FOB (in million $)'],
                  //HS_CODE: obj['HS Code'],
                  //TASK_DATE : taskDate
                }
                newObj["EXTRA_DETAILS"] = [{
                  'Department': obj.Department,
                  'GST/ Establishment Number': obj['GST/ Establishment Number'],
                  'Contact Number': obj['Contact Number'],
                  'DIN': obj['DIN'],
                  'Contact Number': obj['Contact Number'],
                  'Email ID': obj['Email ID']
                }]
                currentObj = { ...newObj };
                delete expObj['Department']
                delete expObj['GST/ Establishment Number']
                delete expObj['Contact Number']
                delete expObj['DIN']
                delete expObj['Contact Number']
                delete expObj['Email ID']
                console.log('exportereinfgffffoo',currentObj);
              }else{
                not_added.push(obj.EXPORTER_NAME)
              }
            }  else {
              currentObj["EXTRA_DETAILS"] = currentObj.EXTRA_DETAILS.concat(obj)
              //get the exporter code
            }
          }
        }
        result.push(currentObj);
        let finalarr = []
       
        const crmAssigned = await ExporterModelV2.aggregate([
          {
            '$match': {
              'EXPORTER_NAME': {
                '$in': result.map(item => item.EXPORTER_NAME)
              }
            }
          },
          {
            '$lookup': {
              from: "tbl_crm_tasks_logs_prod",
              localField: "EXPORTER_NAME",
              foreignField: "EXPORTER_NAME",
              as: "crm_task_logs",
            }
          }, {
            '$project': {
              LOG_TYPE: {
                $first: "$crm_task_logs.LOG_TYPE",
              },
              ADMIN_ID:  {
                $first: "$TASK_ASSIGNED_TO.id"
              },
              EXPORTER_NAME: 1
            }
          },
          {
            $match : {
              ADMIN_ID: {
                $ne:null
              }
            }
          },
          {
            '$group': {
              '_id': '$EXPORTER_NAME', 
              'total_assigned': {
                '$sum': 1
              }, 
              'tasks_not_created': {
                '$sum': {
                  '$cond': [
                    {
                      '$eq': [
                        {
                          '$type': '$LOG_TYPE'
                        }, 'missing'
                      ]
                    }, 1, 0
                  ]
                }
              }
            }
          },
          {
            '$group':{
              '_id': null,
              'total_assigned_gt_0': {
                '$sum': {
                  '$cond': [
                    {
                      '$gt': ['$total_assigned', 0]
                    }, 1, 0
                  ]
                }
              },
              'tasks_not_created_gt_0': {
                '$sum': {
                  '$cond': [
                    {
                      '$gt': ['$tasks_not_created', 0]
                    }, 1, 0
                  ]
                }
              }
        }
          }
        ])
        let finalObj = {
          "Total Exporters" : result.length,
          "Total Assigned Exporters" : crmAssigned?.[0]?.["total_assigned_gt_0"] || 0,
          "Task Not Created": crmAssigned?.[0]?.["tasks_not_created_gt_0"] || 0,
        }
        
        resolve({
          success:true,
          message: finalObj
        })
      }

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}

exports.excelSummaryAdminWise = async (req,res) => {
  try{
    const result = await excelSummaryAdminWiseFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const excelSummaryAdminWiseFunc = ({ showImports }, reqFiles) => {``
  return new Promise(async(resolve, reject) => {
    try {
      let filepath = ''
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        let shwImp = showImports == 'false' ? false : true
        console.log('sheet namesssss',shwImp);
        const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
                const result = [];
        let currentObj = {};
        let not_added = []
        for (let i = 0; i <= data.length - 1; i++) {
          let obj = data[i]
          if (obj) {
            if (obj["EXPORTER_CODE"]) {
              if (Object.keys(currentObj).length) {
                result.push(currentObj);
              }
              const newObj = {
                EXPORTER_CODE: obj.EXPORTER_CODE,
                EXPORTER_NAME: obj.EXPORTER_NAME,
                EXPORTER_ADDRESS: obj.EXPORTER_ADDRESS,
                EXPORTER_CITY: obj.EXPORTER_CITY,
                TOTAL_BUYERS: obj.TOTAL_BUYERS,
                // FOB: obj.FOB,
                // FOB_IN_MILLION: obj['FOB (in million $)'],
                HS_CODE: obj['HS Code'],
                //TASK_DATE : taskDate
              }
              newObj["EXTRA_DETAILS"] = [{
                'Department': obj.Department,
                'GST/ Establishment Number': obj['GST/ Establishment Number'],
                'Contact Number': obj['Contact Number'],
                'DIN': obj['DIN'],
                'Contact Number': obj['Contact Number'],
                'Email ID': obj['Email ID']
              }]
              currentObj = { ...newObj };
              delete obj['Department']
              delete obj['GST/ Establishment Number']
              delete obj['Contact Number']
              delete obj['DIN']
              delete obj['Contact Number']
              delete obj['Email ID']
            } else if(obj["EXPORTER_NAME"]){
              const exporterinfo =await ExporterModel.find({EXPORTER_NAME : {
                $regex : new RegExp(obj.EXPORTER_NAME),
                $options: 'i'
              }})
              let expObj = exporterinfo?.[0]
              if(expObj){
                if (Object.keys(currentObj).length) {
                  result.push(currentObj);
                }
                const newObj = {
                  EXPORTER_CODE: expObj.EXPORTER_CODE,
                  EXPORTER_NAME: expObj.EXPORTER_NAME,
                  EXPORTER_ADDRESS: expObj.EXPORTER_ADDRESS,
                  EXPORTER_CITY: expObj.EXPORTER_CITY,
                  TOTAL_BUYERS: expObj.TOTAL_BUYERS,
                  // FOB: obj.FOB,
                  // FOB_IN_MILLION: obj['FOB (in million $)'],
                  //HS_CODE: obj['HS Code'],
                  //TASK_DATE : taskDate
                }
                newObj["EXTRA_DETAILS"] = [{
                  'Department': obj.Department,
                  'GST/ Establishment Number': obj['GST/ Establishment Number'],
                  'Contact Number': obj['Contact Number'],
                  'DIN': obj['DIN'],
                  'Contact Number': obj['Contact Number'],
                  'Email ID': obj['Email ID']
                }]
                currentObj = { ...newObj };
                delete expObj['Department']
                delete expObj['GST/ Establishment Number']
                delete expObj['Contact Number']
                delete expObj['DIN']
                delete expObj['Contact Number']
                delete expObj['Email ID']
                console.log('exportereinfgffffoo',currentObj);
              }else{
                not_added.push(obj.EXPORTER_NAME)
              }
            }  else {
              currentObj["EXTRA_DETAILS"] = currentObj.EXTRA_DETAILS.concat(obj)
              //get the exporter code
            }
          }
        }
        result.push(currentObj);
        let finalarr = []
        for(let i=0; i<=subadmins.length -1;i++){
          let subadmin = subadmins[i]
          const crmAssigned = await ExporterModelV2.aggregate([
          {
            '$match': {
              'EXPORTER_NAME': {
                '$in': result.map(item => item.EXPORTER_NAME)
              }
            }
          },
          {
            '$lookup': {
              from: "tbl_crm_tasks_logs_prod",
              localField: "EXPORTER_NAME",
              foreignField: "EXPORTER_NAME",
              as: "crm_task_logs",
            }
          }, {
            '$project': {
              LOG_TYPE: {
                $first: "$crm_task_logs.LOG_TYPE",
              },
              ADMIN_ID: {
                $first: "$TASK_ASSIGNED_TO.id"
              },
              EXPORTER_NAME: 1
            }
          },
          {
            $match : {
              ADMIN_ID: subadmin.tbl_user_id,
            }
          },
          {
            '$group': {
              '_id': '$EXPORTER_NAME', 
              'total_assigned': {
                '$sum': 1
              }, 
              'tasks_not_created': {
                '$sum': {
                  '$cond': [
                    {
                      '$eq': [
                        {
                          '$type': '$LOG_TYPE'
                        }, 'missing'
                      ]
                    }, 1, 0
                  ]
                }
              }
            }
          },
          {
            '$group':{
              '_id': null,
              'total_assigned_gt_0': {
                '$sum': {
                  '$cond': [
                    {
                      '$gt': ['$total_assigned', 0]
                    }, 1, 0
                  ]
                }
              },
              'tasks_not_created_gt_0': {
                '$sum': {
                  '$cond': [
                    {
                      '$gt': ['$tasks_not_created', 0]
                    }, 1, 0
                  ]
                }
              }
        }
          }
          ])
          finalarr.push(crmAssigned?.[0]?.["total_assigned_gt_0"] || 0)
        }

        
        resolve({
          success:true,
          message: finalarr
        })
      }

    }catch(e){
      console.log('error in addTask',e)
      reject({
        success:false
      })
    }
  })
}

exports.getfolderWisedata = async (req,res) => {
  try{
    const result = await getfolderWisedataFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getfolderWisedataFunc = ({onlyShowForUserId,search}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let  pipeline = [
        {
          $match: {
            "TASK_ASSIGNED_TO.id": {
              $ne: null,
            },
          },
        },
        
      ]
      if (search) {
        let matchQuery;
    
        if (!isNaN(search)) { // Check if search is a number
            matchQuery = { 'EXTRA_DETAILS.Contact Number': { $regex: new RegExp(search), $options: 'i' } };
        } else if (/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/.test(search)) { // Check if search is an email address
            matchQuery = { 'EXTRA_DETAILS.Email ID': { $regex: new RegExp(search), $options: 'i' } };
        } else {
            matchQuery = {
                $or: [
                    { EXPORTER_NAME: { $regex: new RegExp(search), $options: 'i' } },
                    { EXPORTER_ADDRESS: { $regex: new RegExp(search), $options: 'i' } }
                ]
            };
        }
    
        pipeline.push({
            $match: matchQuery
        });
    }
      if(onlyShowForUserId){
        pipeline.push({
          $match: {
            "TASK_ASSIGNED_TO.id": {$in: onlyShowForUserId}
          }
        })
      }
      pipeline = [...pipeline, {
        $lookup: {
          from: env === 'dev' ? "tbl_crm_tasks_logs" : 'tbl_crm_tasks_logs_prod',
          localField: "EXPORTER_NAME",
          foreignField: "EXPORTER_NAME",
          as: "CRM_TASKS",
        },
      },
      {
        $group: {
          _id: {
            folder_name: "$FOLDER_NAME",
            task_assigned_to: {
              $first:
                "$TASK_ASSIGNED_TO.contact_person",
            },
            tbl_user_id: {
              $first: "$TASK_ASSIGNED_TO.id",
            },
          },
          connected_exporters: {
            $sum: {
              $cond: {
                if: {
                  $gt: [
                    {
                      $size: {
                        $ifNull: ["$CRM_TASKS", []],
                      },
                    },
                    0,
                  ],
                },
                then: 1,
                else: 0,
              },
            },
          },
          not_connected_exporters: {
            $sum: {
              $cond: {
                if: {
                  $eq: [
                    {
                      $size: {
                        $ifNull: ["$CRM_TASKS", []],
                      },
                    },
                    0,
                  ],
                },
                then: 1,
                else: 0,
              },
            },
          },
          expCnt: {
            $sum: 1,
          },
        },
      },
      {
      $lookup: {
          from: "tbl_crm_tasks_assignment",
          let: {
            folderName: "$_id.folder_name",
            tbl_user_id: "$_id.tbl_user_id",
          },
          pipeline: [
            {
              $match: {
                $expr: {
                  $and: [
                    {
                      $eq: [
                        "$activeFolderName",
                        "$$folderName",
                      ],
                    },
                    {
                      $eq: [
                        "$tbl_user_id",
                        "$$tbl_user_id",
                      ],
                    },
                  ],
                },
              },
            },
          ],
          as: "tbl_crm_assignments",
        },
      },
      {
        $addFields: {
          isActive: {
            $cond: {
              if: {
                $eq: [
                  {
                    $size: {
                      $ifNull: ["$tbl_crm_assignments", []],
                    },
                  },
                  0,
                ],
              },
              then: false,
              else: true,
            },
          },
        },
      },
      {
        $group: {
          _id: "$_id.folder_name",
          assignedCounts: {
            $sum: "$expCnt",
          },
          tasks: {
            $push: {
              contact_person: "$_id.task_assigned_to",
              connected_exporters:
                "$connected_exporters",
              not_connected_exporters:
                "$not_connected_exporters",
              tbl_user_id: "$_id.tbl_user_id",
              isActive: "$isActive",
            },
          },
        },
      },
      {
        $lookup: {
          from: "tbl_crm_folders",
          localField: "_id",
          foreignField: "folderName",
          as: "folder",
        },
      },
      {
        $project: {
          _id: 0,
          folder_name: "$_id",
          tasks: 1,
          assigned_by: {
            $first: {
              $ifNull: [
                "$folder.assignedByName",
                null,
              ],
            },
          },
          assigned_at: {
            $first: {
              $ifNull: [
                "$folder.assignmentDate",
                null,
              ],
            },
          },
          filters: {
            $first: {
              $ifNull: ["$folder.filters", null],
            },
          },
          assignedById: {
            $first: {
              $ifNull: ["$folder.assignedById", null],
            },
          },
          updatedAt: {
            $first: {
              $ifNull: ["$folder.updatedAt", null],
            },
          },
          updatedBy: {
            $first: {
              $ifNull: ["$folder.updatedBy", null],
            },
          },
          assignedCounts: 1,
        },
      },{
        $sort: {
          assigned_at: -1
        }
      }]
      console.log('Foldersssss',JSON.stringify(pipeline))
      const response = await ExporterModelV2.aggregate(pipeline)
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      console.log('erroor in db',e)
      reject({
        success:false,
        message : 'Failed to fetch the data'
      })
    }
  })
}


exports.uploadnewFolder = async (req,res) => {
  try{
    const result = await uploadnewFolderFunc(req.body,req.files)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const uploadnewFolderFunc = ({admins, STATUS,FOLDER_NAME,ASSIGNEE_NAME,ASSIGNEE_ID,FILTERS,TASK_TYPE},reqFiles) => {
  return new Promise(async(resolve,reject) =>{
    try{
      resolve({
        success:true,
        message:'Data Assignment started, Please while we are assigning data to users. This may take a while'
      })
      let filepath = ''
      const result = [];
      if (reqFiles && Object.keys(reqFiles).length) {
        Object.keys(reqFiles).forEach(item => {
          let fileHash = reqFiles[item].md5
          filepath = './docs/' + fileHash + '.xlsx'
          fs.writeFileSync(filepath, reqFiles[item].data);
        });
        const workbook = XLSX.readFile(filepath);
        const sheet_name_list = workbook.SheetNames;
        sheet_name_list.forEach(async (sheetname) => {
          let FOLDERNAME = FOLDER_NAME + " - "  + sheetname 
          const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
          let currentObj = {};
          let isCRMDownloadedSheet = true
          let not_added = []
          for (let i = 0; i <= data.length - 1; i++) {
            let obj = data[i]
            if (obj) {
              if(obj["Contact Persons"]){
                  isCRMDownloadedSheet =false
                  const personArr = extractFullNames(obj["Contact Persons"])
                  
                  const phoneArr = obj["Contact Number"]?.toString()?.split(",") || []
                  const emailArr = obj["Email ID"]?.split(",") || []
                  let extraObj=[]
                  for(let k=0;k<=personArr.length - 1; k+=2 ){
                    extraObj.push({
                      "Contact Person" : personArr[k],
                      "Designation": designations.includes(personArr[k+1]) ? personArr[k+1] : ''  
                    })
                    if(!designations.includes(personArr[k+1])){
                      extraObj.push({
                        "Contact Person" : personArr[k+1],
                        "Designation": designations.includes(personArr[k+2]) ? personArr[k+2] : ''  
                      })
                    }
                  }
                  for(let k=0;k<=phoneArr.length - 1; k++ ){
                    if(extraObj[k]){
                      extraObj[k] = {
                        ...extraObj[k],
                        "Contact Number" : phoneArr[k]
                      }
                    }else{
                      extraObj.push({
                        "Contact Number" : phoneArr[k]
                      })
                    }
                  }
                  for(let k=0;k<=emailArr.length - 1; k++ ){
                    if(extraObj[k]){
                      extraObj[k] = {
                        ...extraObj[k],
                        "Email ID" : emailArr[k]
                      }
                    }else{
                      extraObj.push({
                        "Email ID" : emailArr[k]
                      })
                    }
                  }
                  let newobj = {
                    EXPORTER_NAME: obj.EXPORTER_NAME,
                    EXPORTER_ADDRESS: obj.EXPORTER_ADDRESS,
                    EXPORTER_CITY: obj.EXPORTER_CITY,
                    TOTAL_BUYERS: obj.TOTAL_BUYERS,
                    FOB: obj.FOB,
                    FOB_IN_MILLION: obj['FOB (in million $)'],
                    HS_CODE: obj['HS Code'],
                    IEC_NO: obj['IEC_NO'],
                    Remark: obj['Remark'],
                    EXTRA_DETAILS:extraObj,
                    FAX:obj['FAX']
                    //TASK_DATE : taskDate
                  }
                  result.push(newobj)
                }else{
                if (obj["EXPORTER_NAME"]) {
                  if (Object.keys(currentObj).length) {
                    result.push(currentObj);
                  }
                  const newObj = {
                    EXPORTER_CODE: obj.EXPORTER_CODE,
                    EXPORTER_NAME: obj.EXPORTER_NAME,
                    EXPORTER_ADDRESS: obj.EXPORTER_ADDRESS,
                    EXPORTER_CITY: obj.EXPORTER_CITY,
                    TOTAL_BUYERS: obj.TOTAL_BUYERS,
                    FOB: obj.FOB,
                    FOB_IN_MILLION: obj['FOB (in million $)'],
                    HS_CODE: obj['HS Code'],
                    //TASK_DATE : taskDate
                  }
                  newObj["EXTRA_DETAILS"] = [{
                    'Department': obj.Department,
                    'GST/ Establishment Number': obj['GST/ Establishment Number'],
                    'Contact Number': obj['Contact Number'],
                    'DIN': obj['DIN'],
                    'Contact Number': obj['Contact Number'],
                    'Email ID': obj['Email ID']
                  }]
                  currentObj = { ...newObj };
                  delete obj['Department']
                  delete obj['GST/ Establishment Number']
                  delete obj['Contact Number']
                  delete obj['DIN']
                  delete obj['Contact Number']
                  delete obj['Email ID']
                } else if(obj["EXPORTER_NAME"]){
                  const exporterinfo =await ExporterModel.find({EXPORTER_NAME : {
                    $regex : new RegExp(obj.EXPORTER_NAME),
                    $options: 'i'
                  }})
                  let expObj = exporterinfo?.[0]
                  if(expObj){
                    if (Object.keys(currentObj).length) {
                      result.push(currentObj);
                    }
                    const newObj = {
                      EXPORTER_CODE: expObj.EXPORTER_CODE,
                      EXPORTER_NAME: expObj.EXPORTER_NAME,
                      EXPORTER_ADDRESS: expObj.EXPORTER_ADDRESS,
                      EXPORTER_CITY: expObj.EXPORTER_CITY,
                      TOTAL_BUYERS: expObj.TOTAL_BUYERS,
                      FOB: obj.FOB,
                      FOB_IN_MILLION: obj['FOB (in million $)'],
                      HS_CODE: obj['HS Code'],
                      //TASK_DATE : taskDate
                    }
                    newObj["EXTRA_DETAILS"] = [{
                      'Department': obj.Department,
                      'GST/ Establishment Number': obj['GST/ Establishment Number'],
                      'Contact Number': obj['Contact Number'],
                      'DIN': obj['DIN'],
                      'Contact Number': obj['Contact Number'],
                      'Email ID': obj['Email ID']
                    }]
                    currentObj = { ...newObj };
                    delete expObj['Department']
                    delete expObj['GST/ Establishment Number']
                    delete expObj['Contact Number']
                    delete expObj['DIN']
                    delete expObj['Contact Number']
                    delete expObj['Email ID']
                  }else{
                    not_added.push(obj.EXPORTER_NAME)
                  }
                }  else {
                  currentObj["EXTRA_DETAILS"] = currentObj.EXTRA_DETAILS.concat(obj)
                  //get the exporter code
                }
              }
           
            }
          }
          if(isCRMDownloadedSheet){
            result.push(currentObj);
          }
          const leadid = admins
          const query = `SELECT tbl_user_id as id,contact_person,name_title,designation,email_id FROM tbl_user_details WHERE ${leadid.includes("(") ? `tbl_user_id IN ${leadid}` : `tbl_user_id = '${leadid}'`}`
          const dbRes = await call({query},'makeQuery','get')
          const LeadAssignedObj = dbRes.message
          const EXPORTER_LIST = result
          let updateCount = 0
          let newinsertCount = 0
          let expCodes = []
          console.log('Started For Folder ',FOLDERNAME )
          for(let j=0 ; j <= EXPORTER_LIST.length - 1 ;j++){
            let element = EXPORTER_LIST[j]
            const EXPORTER_NAME = EXPORTER_LIST[j].EXPORTER_NAME 
            const crmTasks = await ExporterModelV2.find({ EXPORTER_NAME: { $regex: new RegExp(escapeRegExp(EXPORTER_NAME), 'i') } })
            expCodes.push(EXPORTER_NAME)
            if(crmTasks.length >= 1){
              const crm_extra_details = crmTasks?.[0]?.EXTRA_DETAILS || []
              let combinedArry = [...crm_extra_details,...element.EXTRA_DETAILS]
              //Data Already Exists just update the assignee
              try{
                const res = await ExporterModelV2.updateOne({EXPORTER_NAME:EXPORTER_NAME}, {$set :{TASK_ASSIGNED_TO:LeadAssignedObj,TASK_TYPE:TASK_TYPE,FOLDER_NAME: FOLDERNAME,EXTRA_DETAILS:combinedArry } })
                if(element['Remark']){
                  let ele = crmTasks[0]
                  await CRMTasksLogs.create({
                    EXPORTER_CODE : ele["EXPORTER_CODE"],
                    EXPORTER_NAME : ele["EXPORTER_NAME"],
                    REMARK: element['Remark'],
                    LOG_TYPE:"Create New Task",
                    ADMIN_ID:ASSIGNEE_ID,
                    ADMIN_NAME:ASSIGNEE_NAME
                  })
                }
                //console.log('ModifiedCount',res.modifiedCount,res.matchedCount,EXPORTER_CODE);
                updateCount += res.modifiedCount
              }catch(e) {
                console.log('error in ', e);
              }
            }else{
              let exporterCode = Math.floor(10000000 + Math.random() * (99999999 - 10000000 + 1))
              const crmObj = {
                EXPORTER_CODE:exporterCode,
                EXPORTER_NAME: element.EXPORTER_NAME,
                EXPORTER_ADDRESS: element.EXPORTER_ADDRESS,
                EXPORTER_CITY: element.EXPORTER_CITY,
                TOTAL_BUYERS: element.TOTAL_BUYERS,
                TOTAL_SHIPMENTS:0,
                FOB: element.FOB,
                HS_CODES:[],
                EXTRA_DETAILS:element.EXTRA_DETAILS,
                TASK_ASSIGNED_TO:LeadAssignedObj,
                STATUS:STATUS ? STATUS : 0,
                TASK_TYPE,
                EXPORTER_COUNTRY_CODE:"IN",
                EXPORTER_REGION:"Asia",
                EXPORTER_COUNTRY:"India",
                TASK_TYPE:TASK_TYPE,
                TASK_DATE:null,
                FOLDER_NAME:FOLDERNAME
              }
              crmObj["TOP_COUNTRIES"] = []
              try{
                await ExporterModelV2.create(crmObj) 
                if(element['Remark']){
                  let ele = crmTasks[0]
                  await CRMTasksLogs.create({
                    EXPORTER_CODE : exporterCode,
                    EXPORTER_NAME : ele["EXPORTER_NAME"],
                    REMARK: element['Remark'],
                    LOG_TYPE:"Create New Task",
                    ADMIN_ID:ASSIGNEE_ID,
                    ADMIN_NAME:ASSIGNEE_NAME
                  })
                } 
                newinsertCount += 1 
              }catch(e){
                console.log('EXP NT',element.EXPORTER_NAME)
                continue
              }
            }
          }
          const folder = await CRMFolder.find({folderName: FOLDERNAME})
          if(folder.length > 0){
            let existingExpCodes = folder[0]["assignedCodes"]
            let updatedExpCodes =addUniqueElements(existingExpCodes,expCodes)
            await CRMFolder.updateOne({folderName:FOLDERNAME},{$set:{updatedAt:new Date(),updatedBy:ASSIGNEE_NAME,assignedCodes:updatedExpCodes}})
            console.log('Done For Folder ',FOLDERNAME )

          }else{
            await CRMFolder.create({
              folderName:FOLDERNAME,
              assignedByName:ASSIGNEE_NAME,
              assignedById:ASSIGNEE_ID,
              assignmentDate: new Date(),
              filters:FILTERS,
              assignedCodes:expCodes,
              updatedAt: new Date(),
              updatedBy : ASSIGNEE_NAME
            })
          }
          console.log('Final Count',newinsertCount,updateCount,result.length)
        })
       
        
      }
    }catch(e){
      console.log('error in addmASTERtASK',e);
      reject({
        success:false,
        message:'Failed to assign data'
      })
    }
  })
}

exports.assignCallListFolders = async (req,res) => {
  try{
    const {userId,activeFolderName,action} = req.body
    if(userId){
      const dbRes = await CRMTaskAssignment.find({tbl_user_id : userId})
      if(dbRes?.length){
        //update daily Tasks
        await CRMTaskAssignment.updateOne({tbl_user_id:userId},{$set: {activeFolderName:action ?  activeFolderName : ''}})
      }else{
        //add New Entry
        await CRMTaskAssignment.create({
          tbl_user_id: userId,
          activeFolderName:action ? activeFolderName : ''
        })
      }
      res.send({
        success:true,
        message:"Folder added for call list"
      })
    }else{
      res.send({
        success:false,
        message:"Provide an User Id"
      })
    }
  }catch(e){
    console.log('error in assigntask',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.checkduplicateEmailId = async(req,res) => {
  try{
    const query = `SELECT id FROM tbl_user WHERE login_id = '${req.body.email_id}'`
    const dbRes = await call({query},'makeQuery','get')
    console.log('emailidddddd',dbRes,query)
    if(dbRes.message.length === 0){
      res.send({
        success:true,
        message:''
      })
    }else{
      res.send({
        success:true,
        message:'Email ID already exists.'
      })
    }

  }catch(e){
    res.send({
      success:true,
      message:'Failed to validate email id'
    })
  }
}

exports.getContactDetailsByName = async(req,res) => {
  try{
    const {EXPORTER_NAME} = req.body
    const extradetails = await ExporterModelV2.aggregate([
      {
        $match: {
          EXPORTER_NAME: EXPORTER_NAME
        }
      },
      {
        $project:{
          _id:0,
          EXTRA_DETAILS:1,
          EXPORTER_CODE:1,
          fromLeadGen: 1
        }
      }
    ])
    let EXTRA_DETAILS = extradetails?.[0]?.EXTRA_DETAILS || []
    let EXPORTER_CODE = extradetails?.[0]?.EXPORTER_CODE || ''
    let fromLeadGen = extradetails?.[0]?.fromLeadGen || ''

    res.send({
      success:true,
      data:{
        EXTRA_DETAILS,
        EXPORTER_CODE,
        fromLeadGen
      }
    })
  }catch(e){
    res.send({
      success:false,
      data:{}
    })
  }
}

exports.fetchContactsFromLeadGen = async (req, res) => {
  try {
    const { EXPORTER_NAME } = req.body
    let apiResp = await apiCallV2('https://api.kscan.in/v3/search/byIdOrName', 'POST',
      { "nameMatch": true, "entitySearch": true, "domainSearch": false, "temporaryKid": false, "nameMatchThreshold": true, "filter": { "name": EXPORTER_NAME } },
      {
        "Content-Type": "application/json",
        "x-karza-key": karzaAPIKey
      })
    // console.log("apiRespppppppppppppppppppp", apiResp);
    let selectedItem = null
    for (let index = 0; index < apiResp?.result.length; index++) {
      const element = apiResp?.result[index];
      if (element.type === "COMPANY" && element.status != "INACTIVE") {
        selectedItem = element
        break
      }
    }
    let dataToStore = []
    if (selectedItem) {
      // Lead gen api call 
      let leadGenApiResp = await apiCallV2('https://api.karza.in/kscan/prod/v1/lead-gen/get-contact-details-mini', 'POST',
        { id: selectedItem?.entityId },
        {
          "Content-Type": "application/json",
          "x-karza-key": 'enaIs6IEHD4wlYG' //fiza account key
        })
      // console.log("leadGenApiRespppppppppppppppppppppp", leadGenApiResp);
      // for HR
      for (let index = 0; index < leadGenApiResp?.result?.curatedContacts?.hrContacts?.length; index++) {
        const element = leadGenApiResp?.result?.curatedContacts?.hrContacts[index];
        dataToStore.push({
          "fromLeadGen": true, "Department": 'Human Resource', 'Contact Number': element?.contact?.[0],
          'Contact Person': element?.name, 'Email ID': leadGenApiResp?.result?.curatedEmails?.hrEmails[index]?.email?.[0]
        })
      }
      // for Finance
      for (let index = 0; index < leadGenApiResp?.result?.curatedContacts?.financeContacts?.length; index++) {
        const element = leadGenApiResp?.result?.curatedContacts?.financeContacts[index];
        dataToStore.push({
          "fromLeadGen": true, "Department": 'Finance', 'Contact Number': element?.contact?.[0],
          'Contact Person': element?.name, 'Email ID': leadGenApiResp?.result?.curatedEmails?.financeEmails[index]?.email?.[0]
        })
      }
      // for Management
      for (let index = 0; index < leadGenApiResp?.result?.curatedContacts?.managementContacts?.length; index++) {
        const element = leadGenApiResp?.result?.curatedContacts?.managementContacts[index];
        dataToStore.push({
          "fromLeadGen": true, "Department": 'Management', 'Contact Number': element?.contact?.[0],
          'Contact Person': element?.name, 'Email ID': leadGenApiResp?.result?.curatedEmails?.managementEmails[index]?.email?.[0],
          "Designation": element?.designation
        })
      }
    }
    console.log("dataToStoreeeeeeeeeee", dataToStore);    
    if (dataToStore?.length) {
      let client = new MongoClient(mongoConnectionString);
      await client.connect();
      const database = client.db('trade_db'); // Replace with your database name
      const collection = database.collection(environment === "prod" ? 'india_export_exporters_list_prod' : 'india_export_exporters_list');
      // Try to update if the document exists
      const result = await collection.updateOne(
        { EXPORTER_NAME },
        {
          $set: { fromLeadGen: true },  // Set 'fromLeadGen' if updating
          $push: { EXTRA_DETAILS: { $each: dataToStore } }  // Append to the existing array
        }
      );
      // If no documents were matched, insert a new one
      if (result.matchedCount === 0) {
        await collection.insertOne({
          EXPORTER_NAME,
          EXTRA_DETAILS: dataToStore,
          fromLeadGen: true
        });
        // console.log("Inserted new document");
      } else {
        // console.log("Updated existing document");
      }
      await client.close()
      res.send({
        success: true
      })
    }
    else {
      res.send({
        success: false,
        "message": "No data found"
      })
    }
  } catch (e) {
    console.log("errorInfetchContactsFromLeadGen", error);
    res.send({
      success: false,
      "message": "Something went wrong"
    })
  }
}

