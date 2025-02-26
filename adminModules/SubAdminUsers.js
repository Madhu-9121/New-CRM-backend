const { dbPool } = require("../../src/database/mysql")
const { call } = require("../../utils/codeHelper")
const moment = require("moment");
const fs = require('fs');
const e = require("cors");
const { nullCheck } = require("../../database/utils/utilFuncs");
const { count } = require("console");
const { sendMail } = require("../../utils/mailer");
const config = require("../../config");
const { emailEnabledBanks, enabledFinanciersForLC, activeUserLogicDurationInWeeks, encryptData, env } = require("../../urlCostants");
const { getCurrentTimeStamp, formatSqlQuery, jsonStr } = require("../../iris_server/utils");
const { getModifiApiToken, getDealInfo } = require("../../src/cronjobs/modifi");
const ExporterModelV2 = require("../../src/database/Models/ExporterModelV2");
const CRMTaskAssignmentLogs = require("../../src/database/Models/CRMTaskAssignmentLogs");
const CRMTasksLogs = require("../../src/database/Models/CRMTaskLogs");

const adminPermissions =[
  // "Exporter",
  // "Financier", 
  // "Importer",
  // "Channel Partner",
  // "Assign Task",
  // "Task Manager Users",
  // "LC Limit",
  // "Invoice Limit",
  // "Invoice Finance",
  // "Invoice Approved Finance",
  // "Payments",
  // "Invoice Financer",
  // "Invoice Channel Partner",
  // "Chat Room",
  "User Management",
  "Accounting Analyst",
  "Task Manager",
  "International Financial Services Manager",
  "Domestic Financial Services Manager",
  "CRM Manager",
  "Finance Specialist",
  "Admin Payments",
  "MasterData Analyst",
  "User credit Administrator",
  "Contract Administrator",
  "Operations Manager",
  "Support",
  "Profile"

]

const crmPermissions = [
  // "Task Manager Enquiry",
  // "Task Manager Call List",
  // "CRM Leads",
  // "CRM Data",
  // "Master Data"
]

exports.getSubAdminUserSummary = async (req, res, next) => {
  try {
    let reqBody = req.body
    let response = []
    let todayDateObj = moment()
    let extraQuery = ""
    let extraSearchQry = " AND tbl_user.status = 1";    
    const {type,isAllSubAdmins} = req.body
    if(reqBody.userId){
      extraQuery = ` AND tbl_user.id = '${reqBody.userId}'`
    }
    if(isAllSubAdmins){
      extraSearchQry = ''
    }
    let newQuery = ''
    if(type === 'Admin'){
      let combinedStr = []
      for(let i = 0; i<= adminPermissions.length - 1; i++){
        combinedStr.push(`tbl_user_details.UserPermissions ->> '$."${adminPermissions[i]}"' is NOT NULL`) 
      }
      newQuery = ` AND (${combinedStr.join(" OR ")})`
    }else if(type === 'CRM'){
      let combinedStr = []
      for(let i = 0; i<= crmPermissions.length - 1; i++){
        combinedStr.push(`tbl_user_details.UserPermissions ->> '$."${crmPermissions[i]}"' is NOT NULL`) 
      }
      newQuery = ` AND (${combinedStr.join(" OR ")})`
    }
    let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
    let query = `SELECT tbl_user_details.contact_person, tbl_user_details.phone_code, 
    tbl_user_details.contact_number, tbl_user_details.user_address,
    tbl_user_details.email_id, tbl_user_details.company_city, tbl_user_details.company_state, 
    tbl_user_details.company_postal_code, tbl_user_details.designation,
    tbl_user_details.tbl_user_id,
    GROUP_CONCAT(tbl_document_details.id) AS kycDocs 
    FROM tbl_user 
    LEFT JOIN tbl_user_details ON 
    tbl_user.id = tbl_user_details.tbl_user_id 
    LEFT JOIN tbl_document_details ON
    tbl_user.id = tbl_document_details.created_by
    WHERE 
    tbl_user.type_id = 1 AND tbl_user.isSubUser = 1 ${extraQuery} ${newQuery} ${extraSearchQry}
    GROUP BY tbl_user.id `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    response = dbRes.message
    for (let index = 0; index < response.length; index++) {
      const element = response[index];
      //total lead assigned
      query = `SELECT id FROM tbl_user WHERE LeadAssignedTo = '${element.tbl_user_id}'  `
      dbRes = await call({ query }, 'makeQuery', 'get');
      element["totalLeadAssigned"] = dbRes.message.length
      //total active lead assigned
      query = `SELECT id FROM tbl_user WHERE LeadAssignedTo = '${element.tbl_user_id}' AND last_login_at >= '${lastActiveDateStr}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      element["totalActiveLeadAssigned"] = dbRes.message.length
      //total inactive lead assigned
      element["totalInactiveLeadAssigned"] = element["totalLeadAssigned"] - element["totalActiveLeadAssigned"]

      // total invoice finance applications
      query = `SELECT
      COUNT(CASE WHEN tbl_invoice_discounting.status NOT IN (3,4,5,6) THEN 1 END) AS ongoing_count,
      COUNT(CASE WHEN tbl_invoice_discounting.status IN (3,4,6) THEN 1 END) AS approved_count,
      COUNT(CASE WHEN tbl_invoice_discounting.status = 5 THEN 1 END) AS rejected_count
      FROM tbl_user
      INNER JOIN tbl_invoice_discounting ON tbl_invoice_discounting.seller_id = tbl_user.id
      WHERE tbl_user.LeadAssignedTo = '${element.tbl_user_id}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      element["totalOngoingFinanceApplication"] = dbRes.message?.[0]?.["ongoing_count"] || 0
      element["totalApprovedFinanceApplication"] = dbRes.message?.[0]?.["approved_count"] || 0
      element["totalRejectedFinanceApplication"] = dbRes.message?.[0]?.["approved_count"] || 0

      // total lc finance applications
      query = `SELECT
      COUNT(CASE WHEN tbl_buyer_required_lc_limit.financeStatus IN (0) THEN 1 END) AS ongoing_count,
      COUNT(CASE WHEN tbl_buyer_required_lc_limit.financeStatus IN (1,3,4) THEN 1 END) AS approved_count,
      COUNT(CASE WHEN tbl_buyer_required_lc_limit.financeStatus = 2 THEN 1 END) AS rejected_count
      FROM tbl_user
      INNER JOIN tbl_buyer_required_lc_limit ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id
      WHERE tbl_user.LeadAssignedTo = '${element.tbl_user_id}' `
      dbRes = await call({ query }, 'makeQuery', 'get');
      element["totalOngoingFinanceApplication"] += dbRes.message?.[0]?.["ongoing_count"] || 0
      element["totalApprovedFinanceApplication"] += dbRes.message?.[0]?.["approved_count"] || 0
      element["totalRejectedFinanceApplication"] += dbRes.message?.[0]?.["approved_count"] || 0

      // total of dibursed value by lead
      query = `SELECT SUM(tbl_disbursement_scheduled.amount) as totalDisbursedAmount 
      FROM tbl_disbursement_scheduled 
      INNER JOIN tbl_invoice_discounting ON 
      tbl_disbursement_scheduled.invRefNo = tbl_invoice_discounting.reference_no
      INNER JOIN tbl_user ON 
      tbl_invoice_discounting.seller_id = tbl_user.id
      WHERE tbl_user.LeadAssignedTo = '${element.tbl_user_id}' AND tbl_disbursement_scheduled.status = 1 `
      dbRes = await call({ query }, 'makeQuery', 'get');
      element["totalDisbursedAmount"] = (dbRes.message?.[0]?.["totalDisbursedAmount"] || 0 )    
    }
    res.send({
      success: true,
      message: response
    })    
  }
  catch (error) {
    console.log("in getSubAdminUserSummary error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getSubAdminUser = async (req, res, next) => {
  try {
    let reqBody = req.body
    let sortString = ` ORDER BY tbl_user.status DESC `
    let havingSearchQry = " HAVING "
    let searchQuery = ""
    let perPageString = "";
    let extraSearchQry = " AND tbl_user.status = 1";
    const {excludeId,onlyUserId,parentId,type,isAllSubAdmins} = req.body
    if(reqBody.resultPerPage && reqBody.currentPage) {
      perPageString = ` LIMIT ${reqBody.resultPerPage} OFFSET ${(reqBody.currentPage - 1) * reqBody.resultPerPage}`;
    } 
    if(isAllSubAdmins){
      extraSearchQry = ''
    }
    if(reqBody.search){
      searchQuery += ` AND (tbl_user_details.contact_person LIKE '%${reqBody.search}%' OR tbl_user_details.email_id LIKE '%${reqBody.search}%') `
    }
    let extraQuery=''
    if(excludeId){
      extraQuery = ` AND tbl_user.id != '${excludeId}'`
    }
    if(onlyUserId){
      extraQuery = ` AND tbl_user.id = '${onlyUserId}'`
    }
    if(parentId){
      let subusersQuery = `SELECT tbl_user_id FROM tbl_crm_managers WHERE functional_manager_id = '${parentId}' OR reporting_manager_id = '${parentId}'`
      const dbRes  =await call({query: subusersQuery},'makeQuery','get')
      let subuserIds = dbRes.message?.map(item => item.tbl_user_id) || []
      subuserIds.push(parentId)
      extraQuery += ` AND ( tbl_user.id IN ('${subuserIds.join("','")}'))`
    }
    let newQuery = ''
    // if(type === 'Admin'){
    //   let combinedStr = []
    //   for(let i = 0; i<= adminPermissions.length - 1; i++){
    //     combinedStr.push(`tbl_user_details.UserPermissions ->> '$."${adminPermissions[i]}"' is NOT NULL`) 
    //   }
    //   newQuery = ` AND (${combinedStr.join(" OR ")})`
    // }else if(type === 'CRM'){
    //   let combinedStr = []
    //   for(let i = 0; i<= crmPermissions.length - 1; i++){
    //     combinedStr.push(`tbl_user_details.UserPermissions ->> '$."${crmPermissions[i]}"' is NOT NULL`) 
    //   }
    //   newQuery = ` AND (${combinedStr.join(" OR ")})`
    // }

    if (type === 'Admin') {
      let combinedStr = [];
      for (let i = 0; i <= adminPermissions.length - 1; i++) {
        combinedStr.push(`JSON_SEARCH(tbl_user_details.UserPermissions, 'one', '${adminPermissions[i]}', null, '$[*].roleName') IS NOT NULL`);
      }
      newQuery = ` AND (${combinedStr.join(" OR ")})`;
    } else if (type === 'CRM') {
      let combinedStr = [];
      for (let i = 0; i <= crmPermissions.length - 1; i++) {
        combinedStr.push(`JSON_SEARCH(tbl_user_details.UserPermissions, 'one', '${crmPermissions[i]}', null, '$[*].roleName') IS NOT NULL`);
      }
      newQuery = ` AND (${combinedStr.join(" OR ")})`;
    }
    let crmManagerQry = ""
    if(reqBody.usersReportToMe?.length){
      crmManagerQry = `
      LEFT JOIN tbl_crm_managers ON
      tbl_crm_managers.tbl_user_id = tbl_user_details.tbl_user_id `
    }
    if(reqBody.usersReportToMe?.length){
      extraQuery += ` AND tbl_crm_managers.tbl_user_id IN (${reqBody.usersReportToMe.join(",")}) AND 
      (tbl_crm_managers.functional_manager_id = ${reqBody.userId} OR tbl_crm_managers.reporting_manager_id = ${reqBody.userId}) `
    }

    let query = `SELECT 
    tbl_user.id,
    tbl_user.id as user_id,
    tbl_user.type_id as type_id,
    tbl_user_details.tbl_user_id,
    tbl_user.password,
    tbl_user.created_at,
    tbl_user.status,
    tbl_user_details.designation,
    tbl_user_details.name_title,
    tbl_user_details.contact_person,
    tbl_user_details.company_name,
    tbl_user_details.phone_code,
    tbl_user_details.contact_number,
    tbl_user_details.email_id,
    tbl_user_details.country_code,
    tbl_user_details.user_address,
    tbl_user_details.aadhar_no,
    tbl_user_details.pan_no,
    tbl_user_details.UserPermissions,
    GROUP_CONCAT(tbl_document_details.id) AS kycDocs

    FROM tbl_user

    LEFT JOIN tbl_user_details ON
    tbl_user.id = tbl_user_details.tbl_user_id

    ${crmManagerQry}

    LEFT JOIN tbl_document_details ON
    tbl_user.id = tbl_document_details.created_by

    WHERE tbl_user.type_id = 1 AND tbl_user.isSubUser=1 ${extraQuery}
    ${searchQuery} ${newQuery} ${extraSearchQry}

    GROUP BY tbl_user.id
    
    ${sortString} ${perPageString} `
    let dbRes = await call({ query }, 'makeQuery', 'get');

    let countQuery = `SELECT 
    tbl_user_details.id

    FROM tbl_user

    LEFT JOIN tbl_user_details ON
    tbl_user.id = tbl_user_details.tbl_user_id

    ${crmManagerQry}

    WHERE tbl_user.type_id = 1 AND tbl_user.isSubUser = 1 ${extraQuery}
    ${searchQuery} ${newQuery} ${extraSearchQry}`

    let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');

    res.send({
      success: true,
      message: {
        data: dbRes.message,
        totalCount: countDbRes.message.length
      }
    })
  }
  catch (error) {
    console.log("in getSubAdminUser error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.onboardSubAdminUser = async (req, res, next) => {
  try {
    let reqBody = req.body
    let reqFiles = req.files
    let query = ""
    let dbRes = null

    let dbObjUser = {
      "tableName": "tbl_user",
      "insertObj": {
        login_id: reqBody.email,
        user_name: reqBody.email,
        password: encryptData(reqBody.password),
        tech_type_id: 0,
        type_id: 1,
        parent_id: 0,
        isSubUser: 1,
        status: 1,
        step: 1,
        domain_key: 0,
        created_by: reqBody.userId,
        modified_by: reqBody.userId
      }
    }
    let dbObjUserRes
    if(reqBody.userIdToUpdate){
      await dbPool.query(`UPDATE tbl_user SET login_id = '${reqBody.email}', user_name = '${reqBody.email}', password = '${encryptData(reqBody.password)}'
      WHERE id = '${reqBody.userIdToUpdate}' `)
    }
    else{
      dbObjUserRes = await call(dbObjUser, 'setData', 'post');
    }
    let userId = reqBody.userIdToUpdate || dbObjUserRes.message.id 

    let bc_variables = {
      "bc_usr_reg_flag": 0,
      "bc_usr_reg_ledger_flag": 0
    }
    let userDetailQuery = {
      "tableName": "tbl_user_details",
      "insertObj": {
          designation: reqBody.designation,
          identifier: `USER${new Date().getTime()}`,
          tbl_user_id: userId,
          address: null,
          user_address: reqBody.userAddress,
          company_city: null,
          company_postal_code: null,
          company_name: reqBody.contactPerson,
          contact_person: reqBody.contactPerson,
          name_title: reqBody.nameTitle,
          kyc_done: 1,
          cin_no: reqBody.cinDocumentName || '',
          gst_vat_no: reqBody.gstDocumentName || "",
          pan_no: reqBody.panDocumentName || "",
          iec_no: reqBody.iecDocumentName || "",
          ifsc_no: reqBody.ifscDocumentName || "",
          license_no: reqBody.licenseName || "",
          email_id: reqBody.email,
          industry_type: reqBody.industryType ? reqBody.industryType : null,
          contact_number: reqBody.contactNo,
          phone_code: reqBody.phoneCode,
          country_code: reqBody.country,
          organization_type: reqBody.organizationType ? reqBody.organizationType : null,
          created_by: reqBody.userId,
          modified_by: reqBody.userId,
          user_avatar: "",
          company_gst_verification: 'true',
          company_gst_result: '',
          company_iec_verification: 'true',
          company_iec_result: '',
          company_cin_verification: 'true',
          company_cin_result: '',
          company_pan_verification: 'true',
          company_pan_result: '',
          aadhar_no: reqBody.aadharDocumentName,
          company_aadhar_verification: 'true',
          company_aadhar_result: '',
          plan_id: 1,
          has_plan: 1,
          ...bc_variables,
          UserPermissions: reqBody.userAccess
      }
    }
    if(reqBody.userIdToUpdate){
      await dbPool.query(formatSqlQuery(`UPDATE tbl_user_details SET designation = ?,
      user_address = ?, company_name = ?, contact_person = ?,
      name_title = ?, email_id = ?, contact_number = ?, phone_code = ?,
      country_code = ?, aadhar_no = ?, pan_no = ?, 
      UserPermissions = ?
      WHERE tbl_user_id = ? `, [reqBody.designation, reqBody.userAddress, reqBody.contactPerson, reqBody.contactPerson,
        reqBody.nameTitle, reqBody.email, reqBody.contactNo, reqBody.phoneCode, reqBody.country, reqBody.aadharDocumentName, reqBody.panDocumentName,
      jsonStr(reqBody.userAccess), reqBody.userIdToUpdate]))

    }
    else{
      await call(userDetailQuery, 'setData', 'post');
    }

    if(!reqBody.userIdToUpdate){
      // Creating tbl_user_details_extra entry
      let userDetailExtraQuery = {
        "tableName": "tbl_user_details_extra",
        "insertObj": {
          tbl_user_id: userId,
          business_address: reqBody.userAddress,
          country_of_incorporation: reqBody.country,
          created_at: new Date(),
          created_by: 0,
          modified_at: new Date(),
          modified_by: 0
        }
      }
      await call(userDetailExtraQuery, 'setData', 'post');
    }

    // Upload kyc document if present
    if (reqFiles?.aadharDocument) {
      await dbPool.query(`DELETE FROM tbl_document_details WHERE doc_name = 'Aadhaar Document' AND created_by = "${userId}" `)
      fs.writeFileSync('./docs/' + reqFiles["aadharDocument"].md5, reqFiles["aadharDocument"].data);
      await dbPool.query(`INSERT INTO tbl_document_details (doc_no, doc_name, file_name, gen_doc_label, file_hash,
        category, mst_doc_id, created_at, created_by, modified_at ) VALUE ("", "Aadhaar Document", "${reqFiles["aadharDocument"].name}", "aadharDocument",
        "${reqFiles["aadharDocument"].md5}", "2", "1", "${getCurrentTimeStamp()}", "${userId}", "${getCurrentTimeStamp()}")`)
    }
    if (reqFiles?.panDocument) {
      await dbPool.query(`DELETE FROM tbl_document_details WHERE doc_name = 'PAN Document' AND created_by = "${userId}" `)
      fs.writeFileSync('./docs/' + reqFiles["panDocument"].md5, reqFiles["panDocument"].data);
      await dbPool.query(`INSERT INTO tbl_document_details (doc_no, doc_name, file_name, gen_doc_label, file_hash,
        category, mst_doc_id, created_at, created_by, modified_at ) VALUE ("", "PAN Document", "${reqFiles["panDocument"].name}", "panDocument",
        "${reqFiles["panDocument"].md5}", "2", "1", "${getCurrentTimeStamp()}", "${userId}", "${getCurrentTimeStamp()}")`)
    }
    if(reqBody.selectedCallers){
      const callerIds = reqBody.selectedCallers?.split(",")
      for(let i = 0; i<=callerIds.length - 1;i++){
        const updateQuery = `UPDATE tbl_user SET parent_id='${userId}' WHERE id='${callerIds[i]}'`
        await dbPool.query(updateQuery)
      }
    }
    const managersQuery = `SELECT * FROM tbl_crm_managers WHERE tbl_user_id = '${userId}'`
    const managerRes = await call({query: managersQuery},'makeQuery','get')
    if(managerRes?.message?.length){
      //update Query
      const updateQuery = `UPDATE tbl_crm_managers SET functional_manager_id=${(reqBody.functionalManager && reqBody.functionalManager != 'null') ? `'${reqBody.functionalManager}'` : '0'}, reporting_manager_id=${(reqBody.reportingManager && reqBody.reportingManager != 'null' ) ?  `'${reqBody.reportingManager}'` : '0'} WHERE tbl_user_id='${userId}'` 
      console.log('updateQuery',updateQuery);
      await dbPool.query(updateQuery)
    }else{
      //insert Query
      const insertQuery = `INSERT INTO tbl_crm_managers( tbl_user_id, functional_manager_id, reporting_manager_id) VALUES ('${userId}','${(reqBody.functionalManager && (reqBody.functionalManager != 'null' && reqBody.functionalManager != 'undefined')) ? reqBody.functionalManager : 0}','${(reqBody.reportingManager && (reqBody.reportingManager != 'null' &&  reqBody.reportingManager !=='undefined')) ? reqBody.reportingManager : 0}')`
      await dbPool.query(insertQuery)
    }
    res.send({
      success: true,
      message: reqBody.userIdToUpdate ? "User updated successfully" : "User onboarded successfully"
    })
  }
  catch (error) {
    console.log("in onboardSubAdminUser error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}

exports.getSubAdminManagers = async (req,res) => {
  try{
    const {userId} = req.body 
    const query = `SELECT * FROM tbl_crm_managers WHERE tbl_user_id = '${userId}'`
    const dbRes = await call({query},'makeQuery','get')
    res.send({
      success:true,
      message: dbRes?.message[0] || {}
    })
  }catch(e){
    console.log('error in getSubAdminManagers',e);
    res.send({
      success:false,
      message:e
    })
  }
}

exports.getTaskUpdateUserWiseGraph = async(req,res) => {
  try{
    const result = await getTaskUpdateUserWiseGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getTaskUpdateUserWiseGraphFunc = ({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let pipelinedata = [
        {
          '$match': {
            'TASK_DATE': {
              '$exists': true
            }
          }
        },
      ]
      if(from  && to){
        pipelinedata.push({
          $match : {
            'TASK_DATE' :{
              $gte: new Date(from),
              $lte: new Date(to)
             }
          }
        })
      }
      if(userIds && userIds.length){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : {$in: userIds}
          }
        })  
      }
      if(onlyShowForUserId){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : onlyShowForUserId
          }
        })
      }
      pipelinedata = [...pipelinedata, {
        $lookup : {
          from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'task_logs'
        }
      },
      {
        '$group': {
          "_id": {
            $first: '$TASK_ASSIGNED_TO.contact_person'
          },
          "admin_name": {
              $first : {
              $first: '$TASK_ASSIGNED_TO.contact_person'
            }
          },
          "not_interested": {
            "$sum": {
              "$cond": [{
                "$eq": ["$STATUS", 2]
              }, 1, 0]
            }
          },
          "onboarded": {
            "$sum": {
              "$cond": [{
                "$eq": ["$STATUS", 4]
              }, 1, 0]
            }
          },
          "lost": {
            "$sum": {
              "$cond": [{
                "$eq": ["$STATUS", 3]
              }, 1, 0]
            }
          },
          "hot": {
            "$sum": {
              "$cond": [{
                "$eq": [{
                  "$first": "$task_logs.EVENT_STATUS"
                }, "Hot (30 days or less)"]
              }, 1, 0]
            }
          },
          "cold": {
            "$sum": {
              "$cond": [{
                "$eq": [{
                  "$first": "$task_logs.EVENT_STATUS"
                }, "Cold (60 days or more)"]
              }, 1, 0]
            }
          },
          "warm": {
            "$sum": {
              "$cond": [{
                "$eq": [{
                  "$first": "$task_logs.EVENT_STATUS"
                }, "Warm (30-60 days)"]
              }, 1, 0]
            }
          },
          "incomplete": {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    {
                      '$type': {
                        $first:  "$task_logs.LOG_TYPE"
                      }
                    }, 'missing'
                  ]
                }, 1, 0
              ]
            }
          }
        }
      },{
        $match: {
          admin_name: { $ne: null }
       }
      }, 
      {
        '$project': {
          '_id': 0, 
          'not_interested': 1, 
          'onboarded': 1, 
          'lost': 1,
          'hot':1,
          'cold':1,
          'warm':1,
          'incomplete':1,
          'admin_name': { $arrayElemAt: [{ $split: ["$admin_name", " "] }, 0] }
        }
      }]
      console.log('adsdassdasdas',JSON.stringify(pipelinedata));
      const response = await ExporterModelV2.aggregate(pipelinedata)
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      console.log('error in getTaskUpdateUserWiseGraph',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getLeadsGraphsUserWise = async(req,res) => {
  try{
    const result = await getLeadsGraphsUserWiseFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getLeadsGraphsUserWiseFunc = ({onlyShowForUserId,from,to,subadminIds}) => {
  return new Promise(async (resolve,reject) => {
    try{
      
      const pipelinedata = [
        {
          '$match': {
            $and : [
              {'TASK_DATE': {
                '$exists': true
              }},
              subadminIds && subadminIds.length ? {'TASK_ASSIGNED_TO.id' : {$in: subadminIds}}: {},
              onlyShowForUserId ? {'TASK_ASSIGNED_TO.id' : onlyShowForUserId} : {},
              from && to ? {
                'TASK_DATE' :{
                  $gte: new Date(from),
                  $lte: new Date(to)
                }
              } : {}
            ]
            
          }
        }, 
        {
          '$match': {
            'STATUS': 1
          }
        },
        
        {
          '$group': {
            "_id": {
              $first: '$TASK_ASSIGNED_TO.contact_person'
            }, 
            "admin_name": {
              $first : {
              $first: '$TASK_ASSIGNED_TO.contact_person'
            }
          },
            'lead_count':{
              '$sum':1
            }
          }
        }, {
          $match: {
            admin_name: { $ne: null }
         }
        },
        {
          '$project': {
            '_id': 0, 
            'lead_count':1,
            'admin_name': { $arrayElemAt: [{ $split: ["$admin_name", " "] }, 0] }
          }
        }
      ]
      const response = await ExporterModelV2.aggregate(pipelinedata)
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getCRMPerformanceUserWise = async(req,res) => {
  try{
    const result = await getCRMPerformanceUserWiseFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getCRMPerformanceUserWiseFunc = ({onlyShowForUserId,from,to,subadminIds}) => {
  return new Promise(async (resolve,reject) => {
    try{
      
      let pipelinedata = [
        {
          '$match': {
            'TASK_DATE': {
              '$exists': true
            },
            'TASK_ASSIGNED_TO.id' : {$exists: true}

          }
        },
      ]
      if(from  && to){
        pipelinedata.push({
          $match : {
            'TASK_DATE' :{
              $gte: new Date(from),
              $lte: new Date(to)
             }
          }
        })
      }
      if(subadminIds && subadminIds.length){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : {$in: subadminIds}
          }
        })  
      }
      if(onlyShowForUserId){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : onlyShowForUserId
          }
        })
      }
      pipelinedata = [...pipelinedata, {
        $lookup : {
          from: env === 'dev' ? 'tbl_crm_tasks_logs' : 'tbl_crm_tasks_logs_prod' ,
          localField: 'EXPORTER_CODE',
          foreignField: 'EXPORTER_CODE',
          as: 'task_logs'
        }
      },
      {
        '$project': {
          "task_logs": {
            "$last": "$task_logs"
          },
          "STATUS": 1,
          "LOG_TYPE": {
            "$last": "$task_logs.LOG_TYPE"
          },
          "EVENT_STATUS": {
            "$last": "$task_logs.EVENT_STATUS"
          },
          "TASK_DATE": 1,
          "EVENT_TIME": {
            "$last": "$task_logs.EVENT_TIME"
          },
          "EXPORTER_CODE": 1,
          admin_name: {
            $first:  '$TASK_ASSIGNED_TO.contact_person'
          }
        }
      }, 
      {
        '$project': {
          task_logs:1,
          STATUS:1,
          LOG_TYPE:1,
          EVENT_STATUS:1,
          TASK_DATE:1,
          EVENT_TIME:1,
          admin_name:{ $arrayElemAt: [{ $split: ["$admin_name", " "] }, 0]}
        }
      },{
        $match : {
          "STATUS": {
            "$in": [0, 1, 2, 3]
          }
        }
      }
      ]
      const response = await ExporterModelV2.aggregate(pipelinedata)
      let finaldata = []
      let obj = {}
      for(let i=0; i<= response.length - 1 ; i++){
        const element = response[i]
          if(element.LOG_TYPE === undefined){
            obj[element.admin_name] = {
              ...obj[element.admin_name],
              tasksInComplete : obj[element.admin_name]?.tasksInComplete ?  obj[element.admin_name].tasksInComplete + 1 : 1,
            }
          }else{
            const TasksLogs = element.task_logs
            if(TasksLogs.LOG_TYPE === 'Lead Lost' || TasksLogs.LOG_TYPE === 'User Onboarded' || TasksLogs.LOG_TYPE === 'Not Interested' || TasksLogs.LOG_TYPE === 'Didnt connect'){
              obj[element.admin_name] = {
                ...obj[element.admin_name],
                tasksCompleted : obj[element.admin_name]?.tasksCompleted ?  obj[element.admin_name].tasksCompleted + 1 : 1
              }
            }
            else if((new Date(TasksLogs.EVENT_TIME).getTime() <= new Date(from).getTime() && (new Date(TasksLogs.EVENT_TIME).getTime() >= new Date(to).getTime()))){
              obj[element.admin_name] = {
                ...obj[element.admin_name],
                tasksCompleted : obj[element.admin_name]?.tasksCompleted ?  obj[element.admin_name].tasksCompleted + 1 : 1
              }            
            }else {
              obj[element.admin_name] = {
                ...obj[element.admin_name],
                tasksInComplete : obj[element.admin_name]?.tasksInComplete ?  obj[element.admin_name].tasksInComplete + 1 : 1,
              }            
            }
          }
      }
      for(let i = 0; i<= Object.keys(obj).length - 1 ; i++){
        let admin_name = Object.keys(obj)[i]
        let task_counts =  Object.values(obj)[i]
        if(admin_name != 'null'){
          finaldata.push({
            admin_name,
            task_complete:task_counts.tasksCompleted || 0,
            task_incomplete:task_counts.tasksInComplete || 0,
          })
        }
      }
      console.log('Responseeeeeeee',obj);
      resolve({
        success:true,
        message:finaldata
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getClosureUserWise = async(req,res) => {
  try{
    const result = await getClosureUserWiseFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getClosureUserWiseFunc = ({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let pipelinedata = [
        {
          '$match': {
            'TASK_DATE': {
              '$exists': true
            },
            'TASK_ASSIGNED_TO': {
              '$exists': true
            }
          }
        },
      ]
      if(from  && to){
        pipelinedata.push({
          $match : {
            'TASK_DATE' :{
              $gte: new Date(from),
              $lte: new Date(to)
             }
          }
        })
      }
      if(userIds && userIds.length){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : {$in: userIds}
          }
        })  
      }
      if(onlyShowForUserId){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : onlyShowForUserId
          }
        })
      }
      pipelinedata = [...pipelinedata, 
        {
          '$lookup': {
            'from': 'tbl_crm_tasks_logs_prod', 
            'localField': 'EXPORTER_CODE', 
            'foreignField': 'EXPORTER_CODE', 
            'as': 'task_logs'
          }
        }, {
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            }, 
            'STATUS': 1, 
            'LOG_TYPE': {
              '$last': '$task_logs.LOG_TYPE'
            }, 
            'EVENT_STATUS': {
              '$last': '$task_logs.EVENT_STATUS'
            }, 
            'TASK_DATE': 1, 
            'EVENT_TIME': {
              '$last': '$task_logs.EVENT_TIME'
            }, 
            'EXPORTER_CODE': 1, 
            'admin_name': {
              '$first': '$TASK_ASSIGNED_TO.contact_person'
            }, 
            'CREATED_AT': 1
          }
        }, {
          '$project': {
            'task_logs': 1, 
            'STATUS': 1, 
            'LOG_TYPE': 1, 
            'EVENT_STATUS': 1, 
            'TASK_DATE': 1, 
            'EVENT_TIME': 1, 
            'CREATED_AT': 1, 
            'admin_name': {
              '$arrayElemAt': [
                {
                  '$split': [
                    '$admin_name', ' '
                  ]
                }, 0
              ]
            }
          }
        }, {
          '$match': {
            'LOG_TYPE': {
              '$in': [
                'User Onboarded', 'Lead Lost'
              ]
            }
          }
        }, {
          '$group': {
            '_id': '$admin_name', 
            'totalDays': {
              '$sum': {
                '$divide': [
                  {
                    '$subtract': [
                      {
                        '$toDate': '$TASK_DATE'
                      }, {
                        '$toDate': '$CREATED_AT'
                      }
                    ]
                  }, 1000 * 60 * 60 * 24
                ]
              }
            }, 
            'count': {
              '$sum': 1
            }, 
            'admin_name': {
              '$first': '$admin_name'
            }
          }
        }, {
          '$match': {
            'admin_name': {
              '$ne': null
            }
          }
        }, {
          '$group': {
            '_id': '$admin_name', 
            'totalDays': {
              '$sum': '$totalDays'
            }, 
            'totalCount': {
              '$sum': '$count'
            }
          }
        }, {
          '$project': {
            '_id': 0, 
            'averageDays': {
              '$round': [
                {
                  '$divide': [
                    '$totalDays', '$totalCount'
                  ]
                }, 1
              ]
            }, 
            'admin_name': '$_id'
          }
        }
      ]
      console.log('adsdassdasdas',JSON.stringify(pipelinedata));
      const response = await ExporterModelV2.aggregate(pipelinedata)
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      console.log('error in getTaskUpdateUserWiseGraph',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getUserSummaryAdminWise = async(req,res) => {
  try{
    const result = await getUserSummaryAdminWiseFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getUserSummaryAdminWiseFunc = async({from,to,userIds,onlyShowForUserId}) => {
  return  new Promise(async(resolve,reject) => {
    try{
      let combinedStr = []
      for(let i = 0; i<= adminPermissions.length - 1; i++){
        combinedStr.push(`tbl_user_details.UserPermissions ->> '$."${adminPermissions[i]}"' is NOT NULL`) 
      }
      newQuery = ` AND (${combinedStr.join(" OR ")})`
      const query = `SELECT tbl_user_details.* FROM tbl_user
        LEFT JOIN tbl_user_details ON
        tbl_user_details.tbl_user_id = tbl_user.id
        WHERE tbl_user.type_id = 1 AND tbl_user.isSubUser = 1 ${newQuery}`
        console.log('querytyyyyyyyy',query);
      const dbRes = await call({query},'makeQuery','get')
      const subadminUsers = dbRes.message
      let response = []
      const fromDate = moment(from).format('YYYY-MM-DD');
      const toDate = moment(to).format('YYYY-MM-DD');
      let dateRangeQueryForInvoice = ` AND tbl_buyer_required_limit.updatedAt >= '${fromDate}' AND tbl_buyer_required_limit.updatedAt <= '${toDate}'  `
      let dateRangeQueryForLC = ` AND tbl_buyer_required_lc_limit.updatedAt >= '${fromDate}' AND tbl_buyer_required_lc_limit.updatedAt <= '${toDate}'  `
      let dateRangeInvFin = ` AND tbl_invoice_discounting.modified_at >= '${fromDate}' AND tbl_invoice_discounting.modified_at <= '${toDate}'`

      for(let i=0; i<=subadminUsers.length - 1 ; i++){
        let element = subadminUsers[i]
        let extraQuery = ` AND tbl_user.LeadAssignedTo = '${element.tbl_user_id}'`
        //Buyers Added
        const buyersAddedQuery = `SELECT COUNT(tbl_buyers_detail.id) as total_buyers FROM tbl_buyers_detail
        LEFT JOIN tbl_user ON tbl_user.id = tbl_buyers_detail.user_id 
        WHERE tbl_user.LeadAssignedTo = '${element.tbl_user_id}' AND tbl_buyers_detail.created_at >= '${fromDate}' AND tbl_buyers_detail.created_at <= '${toDate}'
        `
        const buyerAddedRes = await call({query:buyersAddedQuery},'makeQuery','get')
        let totalBuyers = buyerAddedRes?.message?.[0]?.total_buyers || 0

        //Invoice Limit
        const invlimitQuery = `SELECT tbl_buyer_required_limit.id FROM tbl_buyer_required_limit  
        LEFT JOIN tbl_user ON
        tbl_buyer_required_limit.userId = tbl_user.id WHERE (tbl_buyer_required_limit.termSheetSignedByExporter = 0 OR tbl_buyer_required_limit.termSheetSignedByBank = 0) ${dateRangeQueryForInvoice} ${extraQuery}`
        const invlimitRes = await call({ query :invlimitQuery}, 'makeQuery', 'get');
        let invlimitapplications = invlimitRes?.message?.length || 0

        //Invoice Finance
        const invFinQuery = `SELECT tbl_invoice_discounting.id FROM tbl_invoice_discounting     
        LEFT JOIN tbl_user ON
        tbl_invoice_discounting.seller_id = tbl_user.id WHERE 1 ${dateRangeInvFin} ${extraQuery}`
        const invFinRes = await call({ query:invFinQuery }, 'makeQuery', 'get');
        let invFinApplications = invFinRes?.message?.length || 0

        //LC Limit
        let lclimitQuery = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_user.id = tbl_buyer_required_lc_limit.createdBy  WHERE (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 0 OR tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 0) ${dateRangeQueryForLC} ${extraQuery}`
        const lclimitRes = await call({ query:lclimitQuery }, 'makeQuery', 'get');
        let lclimitApplications = lclimitRes?.message?.length || 0

        //LC Finance
        const lcFinQuery = `SELECT tbl_buyer_required_lc_limit.id FROM tbl_buyer_required_lc_limit LEFT JOIN tbl_user ON tbl_user.id = tbl_buyer_required_lc_limit.createdBy WHERE tbl_buyer_required_lc_limit.invRefNo IS NOT NULL  ${dateRangeQueryForLC} ${extraQuery}`
        const lcFinRes = await call({ query:lcFinQuery }, 'makeQuery', 'get');
        response["lcFinanceApplications"] = dbRes.message.length
        let lcFinApplications = lcFinRes?.message?.length || 0

        response.push({
          admin_name : element.company_name?.split(" ")[0],
          admin_id : element.tbl_user_id,
          buyer_added: totalBuyers,
          lc_limit:lclimitApplications,
          invoice_limit:invlimitapplications,
          lc_discounting:lcFinApplications,
          invoice_discounting:invFinApplications
        })
        
      }
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      console.log('error in getUserSummaryAdminWiseFunc',e);
      reject({
        success:false,
        message:e    
      })
    }
  })
}

exports.getDiscountingAdminWise = async(req,res) => {
  try{
    const result = await getDiscountingAdminWiseFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getDiscountingAdminWiseFunc = async({from,to,userIds,onlyShowForUserId}) => {
  return  new Promise(async(resolve,reject) => {
    try{
      let combinedStr = []
      for(let i = 0; i<= adminPermissions.length - 1; i++){
        combinedStr.push(`tbl_user_details.UserPermissions ->> '$."${adminPermissions[i]}"' is NOT NULL`) 
      }
      newQuery = ` AND (${combinedStr.join(" OR ")})`
      const query = `SELECT tbl_user_details.* FROM tbl_user
        LEFT JOIN tbl_user_details ON
        tbl_user_details.tbl_user_id = tbl_user.id
        WHERE tbl_user.type_id = 1 AND tbl_user.isSubUser = 1 ${newQuery}`
      const dbRes = await call({query},'makeQuery','get')
      const subadminUsers = dbRes.message
      let response = []
      const fromDate = moment(from).format('YYYY-MM-DD');
      const toDate = moment(to).format('YYYY-MM-DD');
      let dateRangeQuery = ` AND tbl_disbursement_scheduled.scheduledOn >= '${fromDate}' AND tbl_disbursement_scheduled.scheduledOn <= '${toDate}'  `
      for(let i=0; i<=subadminUsers.length - 1 ; i++){
        let element = subadminUsers[i]
        let extraQuery = ` AND tbl_user.LeadAssignedTo = '${element.tbl_user_id}'`
        const disbursementQuery = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDisbursed FROM tbl_disbursement_scheduled
        LEFT JOIN tbl_invoice_discounting ON 
        tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
        LEFT JOIN tbl_buyer_required_lc_limit ON
        tbl_buyer_required_lc_limit.id = tbl_disbursement_scheduled.invRefNo
        LEFT JOIN tbl_user ON
        tbl_user.id = COALESCE(tbl_invoice_discounting.seller_id, tbl_buyer_required_lc_limit.createdBy)
        WHERE tbl_disbursement_scheduled.status = 1 ${dateRangeQuery} ${extraQuery}`
        const disbursementRes = await call({ query: disbursementQuery }, 'makeQuery', 'get');
        let totalDisbursedAmount = (disbursementRes.message?.[0]?.["totalDisbursed"] || 0 ) 
        console.log('resssssssssss',element.company_name,totalDisbursedAmount);

        response.push({
          admin_name : element.company_name?.split(" ")[0],
          admin_id : element.tbl_user_id,
          disbursement: totalDisbursedAmount,
        })
        
      }
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      console.log('error in getUserSummaryAdminWiseFunc',e);
      reject({
        success:false,
        message:e    
      })
    }
  })
}

exports.getLeadsListByAdmin = async (req,res) => {
  try{
    const result = await getLeadsListByAdminFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getLeadsListByAdminFunc = ({resultPerPage,currentPage,search,status,applicationStatus,exporterName,contactNumber,contactPerson,leadAssignedTo,companyCity,sortCompanyName,sortContactPerson,sortCompanyCity,sortLeadAssignedTo,sortByDate,onlyShowForUserId,userIds}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let typeIdIn = [8,19,20]
      console.log('TypeId',typeIdIn.join(","));
      let userQuery = ''
      if(onlyShowForUserId){
        userQuery = ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        userQuery = ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}')`
      } 
      // if(sub_user_type_id){
      //   typeIdIn.push(sub_user_type_id)
      // }
      let extraSearchQry = ""
      let todayDateObj = moment()
      let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
      let additionalQryForFinancier = ""
      
      if(status){
        let showActive = status.includes("Active")
        let showInactive = status.includes("Inactive")
        if(showActive && !showInactive){
          extraSearchQry += ` AND (tbl_user.last_login_at >= DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK)) `
        }
        if(!showActive && showInactive){
          extraSearchQry += ` AND (tbl_user.last_login_at IS NULL OR tbl_user.last_login_at  < DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK)) `
        }
      }
      let joinQuery = ""
      if(applicationStatus?.length){
        let limitApps = applicationStatus.includes("Limit Application")
        let financeApps = applicationStatus.includes("Finance Application")
        let rejectedApps = applicationStatus.includes("Rejected Application")
       
        if(limitApps){
          extraSearchQry += ` AND (tbl_buyer_required_limit.selectedFinancier IS NOT NULL AND tbl_buyer_required_limit.termSheet IS NULL) OR 
          (tbl_buyer_required_lc_limit.selectedFinancier IS NOT NULL AND tbl_buyer_required_lc_limit.reqLetterOfConfirmation IS NULL) `
        }
        if(financeApps){
          extraSearchQry += ` AND ( tbl_buyer_required_limit.invRefNo IS NOT NULL) OR 
          (tbl_buyer_required_lc_limit.invRefNo IS NOT NULL) `
        }
        if(rejectedApps){
          extraSearchQry += ` AND (tbl_invoice_discounting.status = 5 OR tbl_buyer_required_lc_limit.financeStatus = 2) `
        }
      }
      if(exporterName){
        extraSearchQry += ` AND tbl_user_details.company_name IN (${exporterName.join(",")})`
      }
      if(contactNumber){
        extraSearchQry += ` AND tbl_user_details.contact_number IN (${contactNumber.join(",")})`
      }
      if(contactPerson){
        extraSearchQry += ` AND tbl_user_details.contact_person IN (${contactPerson.join(",")})`
      }
      if(companyCity){
        extraSearchQry += ` AND tbl_user_details.company_city IN (${companyCity.join(",")})`
      }

      if(leadAssignedTo){
        const query = ` SELECT tbl_user_id FROM tbl_user_details WHERE contact_person IN (${leadAssignedTo.join(",")})`
        const dbRes = await call({query},'makeQuery','get')
        const searhArr = dbRes.message.map(item => item.tbl_user_id)
        let searchString = `('${searhArr.join("','")}')`
        if(searhArr.length >=1  && leadAssignedTo.includes("'Not Assigned'")){
          extraSearchQry += ` AND (tbl_user.LeadAssignedTo IN ${searchString} OR tbl_user.LeadAssignedTo IS NULL)`
        }else if(searhArr.length === 0 && leadAssignedTo.includes("'Not Assigned'")){
          extraSearchQry += ` AND (tbl_user.LeadAssignedTo IS NULL)`
        }
        else if(searhArr){
          extraSearchQry += ` AND tbl_user.LeadAssignedTo IN ${searchString}`
        }
      }
      if(onlyShowForUserId){
        extraSearchQry += ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.LeadAssignedTo IS NULL)`
      }
      let query = `SELECT 
      tbl_user.*, 
      tbl_user_details.company_name, 
      tbl_user_details.contact_person, 
      tbl_user_details.designation, 
      tbl_user_details.contact_number, 
      tbl_user_details.name_title, 
      tbl_user_details.phone_code, 
      tbl_user_details.country_code, 
      tbl_user_details.email_id, 
      tbl_user_details.company_city as company_city, 
      tbl_user_details.user_address,
      tbl_user_details.organization_type,
      subAdminTblUserDetails.company_name as TaskAssignedToName,
      parent_tbl_user.login_id AS parent_email_id, 
      parent_tbl_user.ttvExporterCode AS parent_ttv_exporter_code
    FROM 
      tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id 
      LEFT JOIN tbl_user_details subAdminTblUserDetails ON
      tbl_user.LeadAssignedTo = subAdminTblUserDetails.tbl_user_id
      LEFT JOIN tbl_user AS parent_tbl_user ON
      tbl_user.parent_id = parent_tbl_user.id
      ${joinQuery}
    WHERE 
      (tbl_user.type_id IN (${typeIdIn.join(",")})) ${extraSearchQry} ${additionalQryForFinancier} ${userQuery}`

      let countQuery = `SELECT 
      tbl_user.id 
    FROM 
      tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id 
      ${joinQuery}
    WHERE 
      (tbl_user.type_id IN (${typeIdIn.join(",")})) ${extraSearchQry} ${additionalQryForFinancier} ${userQuery}`
      if(search){
        query += ` AND (tbl_user_details.company_name LIKE '%${search}%' OR tbl_user_details.contact_number LIKE '%${search}%')`
        countQuery += ` AND (tbl_user_details.company_name LIKE '%${search}%' OR tbl_user_details.contact_number LIKE '%${search}%')`
      }
      query += `  GROUP BY tbl_user.id   `
      countQuery += `  GROUP BY tbl_user.id   `
      if(sortCompanyName){
        query += ` ORDER BY tbl_user_details.company_name ${sortCompanyName}`
      }else if(sortContactPerson){
        query += ` ORDER BY tbl_user_details.contact_person ${sortContactPerson}`
      }else if(sortCompanyCity){
        query += ` ORDER BY tbl_user_details.company_city ${sortCompanyCity}`
      }else if(sortLeadAssignedTo){
        query += ` ORDER BY subAdminTblUserDetails.company_name  ${sortLeadAssignedTo}`
      }else if(sortByDate){
        query += ` ORDER BY tbl_user.created_at  ${sortByDate}`
      }
      else{
        query += ' ORDER BY tbl_user.last_login_at DESC'
      }

      if(resultPerPage && currentPage){
        var perPageString = ` LIMIT ${resultPerPage} OFFSET ${(currentPage - 1) * resultPerPage}`;
        query += perPageString
      }  
      //console.log('quiertytytt',query);
      const dbRes = await call({query},'makeQuery','get')
      const dbResCount = await call({query:countQuery},'makeQuery','get')
      let response = dbRes.message
      let finaldata = []
      for(let i = 0; i<= response.length - 1 ; i++){
        let element = response[i]
        let queryy = ` 
        SELECT 
          notification_type, 
          notification_sub_type, 
          notification_description, 
          createdBy,
          tbl_user_id, 
          refid 
        FROM 
          tbl_notification_logs  WHERE tbl_user_id = '${element.id}' ORDER BY createdBy DESC LIMIT 1`
        
        const  notificationRes = await call({query:queryy},'makeQuery','get')
        let notificationObj = notificationRes.message?.[0] || {}
        finaldata.push({
          ...element,
          ...notificationObj 
        })
      }

      resolve({
        success:true,
        message:{
          message:finaldata,
          total_count: dbResCount.message.length
        }
      })
    }catch(e){
      console.log('Error in getexporterList',e)
      reject({
        success:false
      })
    }
  })
}


exports.updatesubadmin = async (req,res) => {
  try{
    let result = {
      success:true,
      message:''
    }
    const {subAdminId, colName,userId} = req.body
    if(userId){
      const query = `UPDATE tbl_user SET ${colName} = '${subAdminId}' WHERE id = '${userId}'`
      await dbPool.query(query)
      result.message = 'Updated data Succesfully'
    }else{
      result.success = false
      result.message = 'Please Provide User Id'
    }
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

exports.getAdminProfileStats =async (req,res) =>  {
  try{
    const result = await getAdminProfileStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getAdminProfileStatsFunc = ({onlyShowForUserId,from,to}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let statsdata = {}

      //----------------------------Task Assigned--------------------------
      let todayDateObj = moment()
      let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
      let extraQueryOnboard = ''
      if(onlyShowForUserId){
        extraQuery = ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
        extraQueryOnboard = ` AND (LeadAssignedTo = '${onlyShowForUserId}' OR SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
  
      if (from && to) {
        query = `SELECT * FROM tbl_user WHERE type_id IN ('8','19','20') AND created_at >= '${from}' AND created_at <= '${to}' ${extraQueryOnboard}`
        dbRes = await call({ query }, 'makeQuery', 'get');
        statsdata["total_assigned"] = dbRes.message.length
  
        query = `SELECT * FROM tbl_user WHERE type_id IN ('8','19','20') AND created_at >= '${from}' AND created_at <= '${to}' 
        AND last_login_at >= '${lastActiveDateStr}' ${extraQueryOnboard}`
        dbRes = await call({ query }, 'makeQuery', 'get');
        statsdata["active_assigned"] = dbRes.message.length
        statsdata["inactive_assigned"] = (statsdata["total_assigned"] - statsdata["active_assigned"])
        
      }

      //--------------------------------Finance------------------------------
      if(onlyShowForUserId){
        extraSearchQry = ` AND (adminDetails.tbl_user_id = '${onlyShowForUserId}' OR adminDetailsSec.tbl_user_id = '${onlyShowForUserId}')`
      }
      let dateQuerylimit = ` AND tbl_buyer_required_limit.createdAt BETWEEN '${from}' AND '${to}'`
      let dateQueryfinance = ` AND tbl_invoice_discounting.created_at BETWEEN '${from}' AND '${to}'`

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
      LEFT JOIN tbl_user_details adminDetailsSec ON
      adminDetailsSec.tbl_user_id = tbl_user.SecondaryLeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE tbl_invoice_discounting.status != 3 AND tbl_invoice_discounting.status != 4 AND tbl_invoice_discounting.status != 5 AND 
      tbl_invoice_discounting.status != 6 ${extraSearchQry} ${dateQueryfinance}`
      statsdata["applied"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      statsdata["applied"] = statsdata["applied"].message.length; 
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
      LEFT JOIN tbl_user_details adminDetailsSec ON
      adminDetailsSec.tbl_user_id = tbl_user.SecondaryLeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) ${extraSearchQry} ${dateQueryfinance}`
      statsdata["approved"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      statsdata["approved"] = statsdata["approved"].message.length; 
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
      LEFT JOIN tbl_user_details adminDetailsSec ON
      adminDetailsSec.tbl_user_id = tbl_user.SecondaryLeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE tbl_invoice_discounting.status = 5 ${extraSearchQry} ${dateQueryfinance}`
      statsdata["rejected"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      statsdata["rejected"] = statsdata["rejected"].message.length; 

      //------------Finance Amount--------------------------
      extraSearchQry = ` AND (tbl_buyer_required_limit.termSheetSignedByExporter = 1 AND 
        tbl_buyer_required_limit.termSheetSignedByBank = 1) `
      if(onlyShowForUserId){
        extraSearchQry += ` AND (adminDetails.tbl_user_id = '${onlyShowForUserId}' OR adminDetailsSec.tbl_user_id = '${onlyShowForUserId}')`
      }
      filterQuery = `SELECT SUM(CAST(JSON_EXTRACT(tbl_buyer_required_limit.selectedQuote, "$.financeLimit") AS DECIMAL(10,2))) as finance_limit
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
          LEFT JOIN tbl_user_details adminDetailsSec ON
          adminDetailsSec.tbl_user_id = tbl_user.SecondaryLeadAssignedTo
          WHERE tbl_buyer_required_limit.buyerId IS NOT NULL AND tbl_buyer_required_limit.limitPendingFrom IS NULL
          ${extraSearchQry} ${dateQuerylimit}
          GROUP BY tbl_share_invoice_quote_request.quoteId `;
      let sum = 0
      filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
      filterDbRes.message?.forEach(item => sum += parseFloat(item.finance_limit || 0 ))
      statsdata["approved_limit_amount"] =sum 


      filterCountQry = `SELECT SUM(tbl_invoice_discounting.amount) AS finance_approved_amt
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
      LEFT JOIN tbl_user_details adminDetailsSec ON
      adminDetailsSec.tbl_user_id = tbl_user.SecondaryLeadAssignedTo
      LEFT JOIN tbl_user_details lenderDetails ON
      tbl_invoice_discounting.lender_id = lenderDetails.tbl_user_id
      WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) ${extraSearchQry} ${dateQueryfinance}`
      statsdata["approved_fin_amount"] = await call({ query: filterCountQry }, 'makeQuery', 'get'); 
      statsdata["approved_fin_amount"] = statsdata["approved_fin_amount"].message?.[0]?.finance_approved_amt || 0

      // total of dibursed value by lead
      query = `SELECT SUM(tbl_disbursement_scheduled.amount) as totalDisbursedAmount 
      FROM tbl_disbursement_scheduled 
      INNER JOIN tbl_invoice_discounting ON 
      tbl_disbursement_scheduled.invRefNo = tbl_invoice_discounting.reference_no
      INNER JOIN tbl_user ON 
      tbl_invoice_discounting.seller_id = tbl_user.id
      WHERE (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}') AND tbl_disbursement_scheduled.status = 1 AND tbl_disbursement_scheduled.scheduledOn BETWEEN '${from}' AND '${to}'`
      dbRes = await call({ query }, 'makeQuery', 'get');
      statsdata["total_disbursed"] = (dbRes.message?.[0]?.["totalDisbursedAmount"] || 0 )


      const buyersAddedQuery = `SELECT COUNT(tbl_buyers_detail.id) as total_buyers FROM tbl_buyers_detail
      LEFT JOIN tbl_user ON tbl_user.id = tbl_buyers_detail.user_id 
      WHERE (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}') AND tbl_buyers_detail.created_at >= '${from}' AND tbl_buyers_detail.created_at <= '${to}'
    `
      const buyerAddedRes = await call({query:buyersAddedQuery},'makeQuery','get')
      statsdata["total_buyers"] = (buyerAddedRes?.message?.[0]?.total_buyers || 0 )

      resolve({
        success:true,
        message:statsdata
      })
    }catch(e){
      console.log('error in stats api',e)
      reject({
        success:false,
        message:'Failed to fetch statistics data'
      })
    }
  })
}


exports.getCRMPerformanceSubAdmin = async(req,res) => {
  try{
    const result = await getCRMPerformanceSubAdminFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getCRMPerformanceSubAdminFunc = ({userIds,from,to}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths = moment(to).diff(from,'month') + 1
      console.log('countForMonths',countForMonths)
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
      
      let pipeline = [
        {
          $match: {
            "ASSIGNED_COUNT.id": {
              $in: userIds
            },
            DATE: {
              $gte: new Date(from),
              $lt: new Date(to),
            },
          },
        },
        {
          $unwind: "$ASSIGNED_COUNT",
        },
        {
          $match: {
            "ASSIGNED_COUNT.id": {
              $in: userIds
            },
          },
        },
        {
          $group: {
            _id: {
              $dateToString: {
                format: dateFormat,
                date: "$DATE",
              },
            },
            total_count: {
              $sum: "$ASSIGNED_COUNT.count",
            },
          },
        },
        {
          $project: {
            _id: 0,
            date: "$_id",
            total_count: 1,
          },
        },
        {
          $sort: {
            date: 1,
          },
        },
      ]
      let finaldata = []

      const assignedTasks = await CRMTaskAssignmentLogs.aggregate(pipeline)
      for (let index = 0; index < assignedTasks.length; index++) {
        const element = assignedTasks[index];

        const taskpipeline = [
          {
            $project: {
              'ADMIN_ID': { $toString: "$ADMIN_ID" },
              'EXPORTER_NAME':1,
              'CREATED_DATE_FORMAT': {
                $dateToString: {
                  format: dateFormat,
                  date: "$CREATED_AT",
                },
              },
            }
          },
          {
            $match: {
              'CREATED_DATE_FORMAT' : element.date,
              'ADMIN_ID': userIds[0]?.toString()
            }
          },
          {
            $group: {
              _id:'$EXPORTER_NAME'
            }
          },
          {
            $count: 'total_connected'
          }
        ]
        const taskconnected = await CRMTasksLogs.aggregate(taskpipeline)
        let taskcomp = taskconnected?.[0]?.total_connected || 0
        finaldata.push({
          label:element.date,
          task_complete:taskcomp,
          task_incomplete:element.total_count < taskcomp ? 0 :element.total_count - taskcomp  
        })
        
      }
      resolve({
        success:true,
        message:finaldata
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getClosureSubadmin = async(req,res) => {
  try{
    const result = await getClosureSubadminFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getClosureSubadminFunc = ({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths = moment(to).diff(from,'month') + 1
      console.log('countForMonths',countForMonths)
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
      let pipelinedata = [
        {
          '$match': {
            'TASK_DATE': {
              '$exists': true
            },
            'TASK_ASSIGNED_TO': {
              '$exists': true
            }
          }
        },
      ]
      if(from  && to){
        pipelinedata.push({
          $match : {
            'TASK_DATE' :{
              $gte: new Date(from),
              $lte: new Date(to)
             }
          }
        })
      }
      if(userIds && userIds.length){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : {$in: userIds}
          }
        })  
      }
      if(onlyShowForUserId){
        pipelinedata.push({
          $match: {
            'TASK_ASSIGNED_TO.id' : onlyShowForUserId
          }
        })
      }
      pipelinedata = [...pipelinedata, 
        {
          '$lookup': {
            'from': env === 'dev' ? 'tbl_crm_tasks_logs': 'tbl_crm_tasks_logs_prod', 
            'localField': 'EXPORTER_CODE', 
            'foreignField': 'EXPORTER_CODE', 
            'as': 'task_logs'
          }
        }, {
          '$project': {
            'task_logs': {
              '$last': '$task_logs'
            }, 
            'STATUS': 1, 
            'LOG_TYPE': {
              '$last': '$task_logs.LOG_TYPE'
            }, 
            'EVENT_STATUS': {
              '$last': '$task_logs.EVENT_STATUS'
            }, 
            'TASK_DATE': 1, 
            'EVENT_TIME': {
              '$last': '$task_logs.EVENT_TIME'
            }, 
            'EXPORTER_CODE': 1, 
            'admin_name': {
              '$first': '$TASK_ASSIGNED_TO.contact_person'
            }, 
            'CREATED_AT': 1
          }
        }, {
          '$project': {
            'task_logs': 1, 
            'STATUS': 1, 
            'LOG_TYPE': 1, 
            'EVENT_STATUS': 1, 
            'TASK_DATE': 1, 
            'EVENT_TIME': 1, 
            'CREATED_AT': 1, 
            'admin_name': {
              '$arrayElemAt': [
                {
                  '$split': [
                    '$admin_name', ' '
                  ]
                }, 0
              ]
            }
          }
        }, {
          '$match': {
            'LOG_TYPE': {
              '$in': [
                'User Onboarded', 'Lead Lost'
              ]
            }
          }
        }, {
          '$group': {
            '_id': {
              $dateToString: {
                format: dateFormat,
                date: "$TASK_DATE",
              },
            }, 
            'totalDays': {
              '$sum': {
                '$divide': [
                  {
                    '$subtract': [
                      {
                        '$toDate': '$TASK_DATE'
                      }, {
                        '$toDate': '$CREATED_AT'
                      }
                    ]
                  }, 1000 * 60 * 60 * 24
                ]
              }
            }, 
            'count': {
              '$sum': 1
            }, 
            'admin_name': {
              '$first': '$admin_name'
            }
          }
        }, {
          '$project': {
            '_id': 0, 
            'averageDays': {
              '$round': [
                {
                  '$divide': [
                    '$totalDays', '$count'
                  ]
                }, 1
              ]
            }, 
            'label': '$_id'
          }
        }
      ]
      console.log('adsdassdasdas',JSON.stringify(pipelinedata));
      const response = await ExporterModelV2.aggregate(pipelinedata)
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      console.log('error in getTaskUpdateUserWiseGraph',e);
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getCRMSubAdminStats = async(req,res) => {
  try{
    const result = await getCRMSubAdminStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getCRMSubAdminStatsFunc = ({from,to,userIds}) => {
  return new Promise(async (resolve,reject) => {
    try{
      const tasksPipeline = [
        {
          $match : {
            'CREATED_AT' :{
              $gte: new Date(from),
              $lte: new Date(to)
             }
          }
        },
        {
          $lookup : {
            from: env === 'dev' ? "india_export_exporters_list" : "india_export_exporters_list_prod",
            localField: "EXPORTER_CODE",
            foreignField: "EXPORTER_CODE",
            as: "crm_tasks",
          }    
        },
        {
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
            },
            EVENT_STATUS:1
          }
        },
        {
          $match: {
            "TASK_ASSIGNED_TO.id":{
              $in: userIds
            }
          }
        }
      ]
      const assignedPipeline = [
        {
          $match: {
            'TASK_DATE': {
              $gte: new Date(from),
              $lte: new Date(to)
            },
            'TASK_ASSIGNED_TO.id': {
              $in: userIds
            }
          }
        },
        {
          $lookup: {
            from: env === 'dev' ? "tbl_crm_tasks_logs" : "tbl_crm_tasks_logs_prod",
            localField: "EXPORTER_CODE",
            foreignField: "EXPORTER_CODE",
            as: "crm_tasks",
          }
        },
        {
          $group: {
            _id: null,
            overdue_exporters: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      { $gt: ['$CREATED_AT', new Date(to).toISOString()] },
                      { $eq: [{ $size: '$crm_tasks' }, 0] }
                    ]
                  },
                  1,
                  0
                ]
              }
            },
            total_exporters : {
              $sum:1
            }
          }
        },
        {
          $project: {
            _id: 0,
            total_exporters_overdue: "$overdue_exporters",
            total_exporters:1
          }
        }
      ]
      const logTypePipeline = [...tasksPipeline]
      const eventPipeline = [...tasksPipeline]
      logTypePipeline.push({
        $group : {
          _id: '$LOG_TYPE',
          'total_records' : {$sum: 1},
          'LOG_TYPE':{$last:'$LOG_TYPE'}
        }
      })

      eventPipeline.push({
        $group : {
          _id: '$EVENT_STATUS',
          'total_records' : {$sum: 1},
          'EVENT_TYPE':{$first : '$EVENT_STATUS'}
        }
      })
      const eventResponse = await CRMTasksLogs.aggregate(eventPipeline)
      const assignedResponse = await ExporterModelV2.aggregate(assignedPipeline)
      const logsResponse = await CRMTasksLogs.aggregate(logTypePipeline)
      console.log('assignedResponse',assignedResponse,JSON.stringify(assignedPipeline))

      let eventdata = [...logsResponse,...eventResponse]
      const result = {};

      eventdata.forEach(item => {
        const id = item._id ;

        if (result[id]) {
          result[id].total_records += item.total_records;
        } else {
          result[id] = {
            label: id,
            total_records: item.total_records
          };
        }
      });
      resolve({
        success:true,
        message:{
          eventdata,
          assignedResponse
        }
      })
    }catch(e){
      console.log('error in API',e)
      resolve({
        success:false,
        message:[]
      })
    }
  })
}
