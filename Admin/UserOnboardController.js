const { dbPool } = require("../../src/database/mysql")
const { call } = require("../../utils/codeHelper")
const fs = require('fs');
const { activeUserLogicDurationInWeeks } = require("../../urlCostants");
const moment = require("moment");
const IECDetails = require("../../src/database/Models/IECDetailsModel");
const { mysqlTextParse } = require("../../iris_server/utils");

exports.getUserManagementFiltersForAdmin = async (req, res, next) => {
  try {
    let reqBody = req.body
    let filterData = {}
    const {search,type_id, sub_user_type_id, status,applicationStatus,onlyShowForUserId} = req.body
    //
    filterData["Status"] = {
      "accordianId": 'status',
      type: "checkbox",
      data: [{name: "Active"},{name: 'Inactive'}],
      labelName: "name"
    }
    //
    filterData["Application Status"] = {
      "accordianId": 'applicationStatus',
      type: "checkbox",
      data: [{name: "Limit Application"},{name: 'Finance Application'},
      {name: "Disbursed"}, {name: "Limit Approved But Not Financed"}, {name: 'Limit Applied But No Updates'},
      {name: "Rejected Application"}],
      labelName: "name"
    }
    let typeIdIn = [type_id]
    if(sub_user_type_id){
      typeIdIn.push(sub_user_type_id)
    }
    let extraSearchQry = ""
    let todayDateObj = moment()
    let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
    if(status){
      let showActive = status.includes("Active")
      let showInactive = status.includes("Inactive")
      if(showActive && !showInactive){
        extraSearchQry += ` AND tbl_user.last_login_at >= DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) `
      }
      if(!showActive && showInactive){
        extraSearchQry += ` AND (tbl_user.last_login_at IS NULL OR tbl_user.last_login_at  < DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) `
      }
    }
    let joinQuery = ""
    if(applicationStatus?.length){
      let limitApps = applicationStatus.includes("Limit Application")
      let financeApps = applicationStatus.includes("Finance Application")
      let rejectedApps = applicationStatus.includes("Rejected Application")
      if(type_id/1==8){
        joinQuery += ` LEFT JOIN tbl_buyer_required_limit ON tbl_user.id = tbl_buyer_required_limit.selectedFinancier
        LEFT JOIN tbl_buyer_required_lc_limit ON tbl_user.id = tbl_buyer_required_lc_limit.selectedFinancier 
        LEFT JOIN tbl_invoice_discounting ON tbl_buyer_required_limit.invRefNo = tbl_invoice_discounting.status
        `
      }
      if(type_id/1==19){
        joinQuery += ` LEFT JOIN tbl_buyer_required_limit ON tbl_user.id = tbl_buyer_required_limit.userId
        LEFT JOIN tbl_buyer_required_lc_limit ON tbl_user.id = tbl_buyer_required_lc_limit.createdBy 
        LEFT JOIN tbl_invoice_discounting ON tbl_buyer_required_limit.invRefNo = tbl_invoice_discounting.status
        `
      }
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
    let query = `SELECT 
    tbl_user_details.company_name, 
    tbl_user_details.contact_person, 
    tbl_user_details.contact_number, 
    tbl_user_details.company_city as company_city, 
    subAdminTblUserDetails.company_name as TaskAssignedToName
  FROM 
    tbl_user 
    LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id 
    LEFT JOIN tbl_user_details subAdminTblUserDetails ON
    tbl_user.LeadAssignedTo = subAdminTblUserDetails.tbl_user_id
    
    LEFT JOIN (
      SELECT 
        notification_type, 
        notification_sub_type, 
        notification_description, 
        createdby, 
        refid 
      FROM 
        tbl_notification_logs 
      GROUP BY 
        createdby 
      limit 
        1
    ) o ON tbl_user.id = o.createdby 
    LEFT JOIN tbl_user AS parent_tbl_user ON
    tbl_user.parent_id = parent_tbl_user.id
    ${joinQuery}
  WHERE 
    (tbl_user.type_id IN (${typeIdIn.join(",")})) ${extraSearchQry}`

    if(search){
      query += ` AND tbl_user_details.company_name LIKE '%${search}%'`
    }
    query += `  GROUP BY tbl_user.id   `
    query += ' ORDER BY tbl_user.last_login_at DESC'

    const dbRes = await call({query},'makeQuery','get')
    const uniqueCompany =  [...new Map(dbRes.message.map(item => [item["company_name"], {name : item.company_name}])).values()].filter(item => item.name).sort((a, b) => a.name?.toLowerCase() > b.name?.toLowerCase() ? 1 : -1);
    const uniqueContactNo = [...new Map(dbRes.message.map(item => [item["contact_number"], {name : item.contact_number}])).values()].filter(item => item.name).sort((a, b) => a.name?.toLowerCase() > b.name?.toLowerCase() ? 1 : -1);
    const uniqueContactPerson = [...new Map(dbRes.message.map(item => [item["contact_person"], {name : item.contact_person}])).values()].filter(item => item.name).sort((a, b) => a.name?.toLowerCase() > b.name?.toLowerCase() ? 1 : -1);
    const uniqueCompanyCity = [...new Map(dbRes.message.map(item => [item["company_city"], {name : item.company_city}])).values()].filter(item => item.name).sort((a, b) => a.name?.toLowerCase() > b.name?.toLowerCase() ? 1 : -1);
    const uniqueLeadAssignedTo = [...new Map(dbRes.message.map(item => [item["TaskAssignedToName"], {name : item.TaskAssignedToName}])).values()].filter(item => item.name).sort((a, b) => a.name?.toLowerCase() > b.name?.toLowerCase() ? 1 : -1);
    filterData["Exporter Name"] = {
      "accordianId": 'exporterName',
      type: "checkbox",
      data: uniqueCompany,
      labelName: "name"
    }
    filterData["Contact Person"] = {
      "accordianId": 'contactPerson',
      type: "checkbox",
      data: uniqueContactPerson,
      labelName: "name"
    }
    filterData["Contact Number"] = {
      "accordianId": 'contactNumber',
      type: "checkbox",
      data: uniqueContactNo,
      labelName: "name"
    }
    filterData["Company City"] = {
      "accordianId": 'companyCity',
      type: "checkbox",
      data: uniqueCompanyCity,
      labelName: "name"
    }    
    if(!onlyShowForUserId){
      filterData["Lead Assigned To"] = {
        "accordianId": 'leadAssignedTo',
        type: "checkbox",
        data: [{name:"Not Assigned"},...uniqueLeadAssignedTo],
        labelName: "name"
      }
    }else{
      const query = `SELECT contact_person as name FROM tbl_user_details WHERE tbl_user_id = '${onlyShowForUserId}'`
      const leadRes = await call({query},'makeQuery','get')
      filterData["Lead Assigned To"] = {
        "accordianId": 'leadAssignedTo',
        type: "checkbox",
        data: [{name:"Not Assigned"},...leadRes.message],
        labelName: "name"
      }
    }

    res.send({
      success: true,
      message: filterData
    })
  }
  catch (error) {
    console.log("in getUserManagementFiltersForAdmin error--->>", error)
    res.send({
      success: false,
      message: error
    })
  }
}


exports.getexportersummaryAdmin = async(req,res) => {
  try{
    const result = await getexportersummaryAdminFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getexportersummaryAdminFunc = ({type_id, sub_user_type_id,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let summaryObj = {
        "total_limit_count":0,
        "total_rejected_count":0,
        "total_finance_count":0,
        "total_exporters":0,
        "active_exporters":0,
        "inactive_exporters":0
      }

      let typeIdIn = [type_id]
      if(sub_user_type_id){
        typeIdIn.push(sub_user_type_id)
      }
      let extraSearchQry = ''
      if(onlyShowForUserId){
        extraSearchQry += ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.LeadAssignedTo IS NULL)`
      }

      let additionalQryForFinancier = ""
      if(type_id/1==8){
        additionalQryForFinancier = ` AND tbl_user.parent_id = 0 `
      }

      const exporterCount = `SELECT COUNT(*) as total_exporters FROM tbl_user WHERE type_id IN (${typeIdIn.join(",")}) ${extraSearchQry} ${additionalQryForFinancier}`
      const dbExporterCount = await call({query:exporterCount},'makeQuery','get')
      summaryObj["total_exporters"] = dbExporterCount.message[0].total_exporters

      const userActivityQuery = `
        SELECT
          SUM(CASE WHEN last_login_at  >= DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) THEN 1 ELSE 0 END) AS active_users_count,
          SUM(CASE WHEN last_login_at IS NULL OR last_login_at  < DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) THEN 1 ELSE 0 END) AS inactive_users_count
        FROM tbl_user WHERE type_id IN (${typeIdIn.join(",")}) ${extraSearchQry} ${additionalQryForFinancier} `

      const dbuserActivityCount = await call({query:userActivityQuery},'makeQuery','get')
      summaryObj["active_exporters"] = dbuserActivityCount.message[0].active_users_count
      summaryObj["inactive_exporters"] = dbuserActivityCount.message[0].inactive_users_count

      const limitApplicationQuery = `SELECT Count(*) as total_quotes FROM tbl_buyer_required_lc_limit 
      LEFT JOIN tbl_buyers_detail ON
      tbl_buyer_required_lc_limit.buyerId = tbl_buyers_detail.id
      LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
      LEFT JOIN tbl_user ON tbl_buyers_detail.user_id = tbl_user.id
      WHERE (tbl_buyer_required_lc_limit.reqLetterOfConfirmation IS NULL OR tbl_buyer_required_limit.termSheet IS NULL) ${extraSearchQry}`

      const dblimitApplicationCount = await call({query:limitApplicationQuery},'makeQuery','get')
      summaryObj["total_limit_count"] = dblimitApplicationCount.message[0].total_quotes

      let totalLimitRejQuery = `SELECT tbl_buyer_required_limit.buyers_credit FROM tbl_buyer_required_limit
      LEFT JOIN tbl_buyers_detail ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
      LEFT JOIN tbl_user ON tbl_buyer_required_limit.userId = tbl_user.id
      WHERE tbl_buyer_required_limit.buyers_credit is NOT NULL ${extraSearchQry}
      
      UNION 
      
      SELECT tbl_buyer_required_lc_limit.financierQuotes FROM tbl_buyer_required_lc_limit
      LEFT JOIN tbl_user ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id
      WHERE tbl_buyer_required_lc_limit.financierQuotes is NOT NULL ${extraSearchQry}`
      const totalLimitRej = await call({query:totalLimitRejQuery},'makeQuery','get')
      let count = 0
      for(let i = 0; i<totalLimitRej.message.length - 1;i++){
        const element = totalLimitRej.message[i]
        const selectedQuote = JSON.parse(element.buyers_credit)
        for(let j=0;j<= selectedQuote.length - 1; j++){
          let obj = selectedQuote[j]
          if(obj.financierAction === 'deny' || obj.status === 'denied' ){
            count += 1
          }
        }
        summaryObj["total_rejected_count"] = count
      }

      const finAppQuery = `SELECT COUNT(*) AS totalFinApplication FROM tbl_invoice_discounting
      LEFT JOIN tbl_user ON tbl_invoice_discounting.seller_id = tbl_user.id
      WHERE 1 ${extraSearchQry}`
      const finAppRes = await call({query:finAppQuery}, 'makeQuery', 'get');
      summaryObj["total_finance_count"] = finAppRes?.message[0]?.totalFinApplication

      resolve({
        success:true,
        message:summaryObj
      })
    }catch(e){
      console.log('error in getadmin exporter summary',e)
      reject({
        success:false,
        message: {}
      })
    }
  })
}

exports.getAllExportersOnPlatform = async (req,res) => {
  try{
    let query = `SELECT tbl_user_details.* FROM tbl_user_details 
    LEFT JOIN tbl_user ON tbl_user.id = tbl_user_details.tbl_user_id 
    WHERE tbl_user.type_id = 19 `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    res.send({
      success: true,
      data: dbRes.message
    })
  }catch(e){
    res.send({
      success: true,
      data: []
    })
  }
}

exports.getExportersListForAdmin = async (req,res) => {
  try{
    const result = await getExportersListForAdminFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getExportersListForAdminFunc = ({resultPerPage,currentPage,search,type_id, sub_user_type_id, status,applicationStatus,exporterName,contactNumber,contactPerson,leadAssignedTo,companyCity,sortCompanyName,sortContactPerson,sortCompanyCity,sortLeadAssignedTo,sortByDate,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let typeIdIn = [type_id]
      if(sub_user_type_id){
        typeIdIn.push(sub_user_type_id)
      }
      let extraSearchQry = ""
      let todayDateObj = moment()
      let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
      let additionalQryForFinancier = ""
      if(type_id/1==8){
        additionalQryForFinancier = ` AND tbl_user.parent_id = 0 `
      }
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
        let disbursedApps = applicationStatus.includes("Disbursed")
        let limitApprovedButNotFinancedApps = applicationStatus.includes("Limit Approved But Not Financed")
        let limitAppliedButNoUpdates = applicationStatus.includes("Limit Applied But No Updates")
        let rejectedApps = applicationStatus.includes("Rejected Application")
        if(type_id/1==8){
          joinQuery += ` LEFT JOIN tbl_buyer_required_limit ON tbl_user.id = tbl_buyer_required_limit.selectedFinancier
          LEFT JOIN tbl_buyer_required_lc_limit ON tbl_user.id = tbl_buyer_required_lc_limit.selectedFinancier 
          LEFT JOIN tbl_invoice_discounting ON tbl_buyer_required_limit.invRefNo = tbl_invoice_discounting.reference_no
          `
        }
        if(type_id/1==19){
          joinQuery += ` LEFT JOIN tbl_buyer_required_limit ON tbl_user.id = tbl_buyer_required_limit.userId
          LEFT JOIN tbl_buyer_required_lc_limit ON tbl_user.id = tbl_buyer_required_lc_limit.createdBy 
          LEFT JOIN tbl_invoice_discounting ON tbl_buyer_required_limit.invRefNo = tbl_invoice_discounting.reference_no
          `
        }
        if(limitApps){
          extraSearchQry += ` AND (tbl_buyer_required_limit.selectedFinancier IS NOT NULL AND tbl_buyer_required_limit.termSheet IS NULL) OR 
          (tbl_buyer_required_lc_limit.selectedFinancier IS NOT NULL AND tbl_buyer_required_lc_limit.reqLetterOfConfirmation IS NULL) `
        }
        if(financeApps){
          extraSearchQry += ` AND ( tbl_buyer_required_limit.invRefNo IS NOT NULL) OR 
          (tbl_buyer_required_lc_limit.invRefNo IS NOT NULL) `
        }
        if(disbursedApps){
          extraSearchQry += ` AND (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) `
        }
        if(limitApprovedButNotFinancedApps){
          extraSearchQry += ` AND (tbl_buyer_required_limit.buyers_credit LIKE '%"financierAction":"Approved"%' AND tbl_invoice_discounting.status NOT IN (3,4,6)) `
        }
        if(limitAppliedButNoUpdates){
          extraSearchQry += ` AND (tbl_buyer_required_limit.id IS NOT NULL AND tbl_buyer_required_limit.buyers_credit IS NULL) `
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
        extraSearchQry += ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.LeadAssignedTo IS NULL)`
      }
      let query = `SELECT 
      tbl_user.*, 
      tbl_user_details.company_name, 
      tbl_user_details.contact_person, 
      tbl_user_details.contact_number, 
      tbl_user_details.name_title, 
      tbl_user_details.phone_code, 
      tbl_user_details.country_code, 
      tbl_user_details.email_id, 
      tbl_user_details.company_city as company_city, 
      tbl_user_details.user_address,
      subAdminTblUserDetails.company_name as TaskAssignedToName,      parent_tbl_user.login_id AS parent_email_id, 
      parent_tbl_user.ttvExporterCode AS parent_ttv_exporter_code,
      tbl_user_tasks_logs.LOG_TYPE AS LastEventType,
      tbl_user_tasks_logs.CREATED_AT AS LastEventTime,
      tbl_user_tasks_logs.REMARK AS LastNote
    FROM 
      tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id 
      LEFT JOIN tbl_user_tasks_logs ON tbl_user_tasks_logs.EXPORTER_CODE = tbl_user.id
      LEFT JOIN tbl_user_details subAdminTblUserDetails ON
      tbl_user.LeadAssignedTo = subAdminTblUserDetails.tbl_user_id
      
      LEFT JOIN tbl_user AS parent_tbl_user ON
      tbl_user.parent_id = parent_tbl_user.id
      ${joinQuery}
    WHERE 
      (tbl_user.type_id IN (${typeIdIn.join(",")})) ${extraSearchQry} ${additionalQryForFinancier}`

      let countQuery = `SELECT 
      tbl_user.id 
    FROM 
      tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id 
      ${joinQuery}
    WHERE 
      (tbl_user.type_id IN (${typeIdIn.join(",")})) ${extraSearchQry} ${additionalQryForFinancier}`
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
      const dbbCount = await call({query},'makeQuery','get')

      if(resultPerPage && currentPage){
        var perPageString = ` LIMIT ${resultPerPage} OFFSET ${(currentPage - 1) * resultPerPage}`;
        query += perPageString
      }  
      //console.log('quiertytytt',query);
    // console.log("queryyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", query);
      const dbRes = await call({query},'makeQuery','get')
      //const dbResCount = await call({query:countQuery},'makeQuery','get')
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
          total_count: dbbCount.message.length
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


exports.getRefferalsList = async (req,res) => {
  try{
    const result = await getRefferalsList(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getRefferalsList = ({resultPerPage,currentPage,search,userId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let query = `SELECT tbl_user_details.contact_person,tbl_user_details.contact_number,tbl_user_details.phone_code,tbl_user_details.name_title,tbl_user.type_id, tbl_user_details.email_id
      FROM tbl_network_requests
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_network_requests.request_to
      LEFT JOIN tbl_user ON tbl_user.id = tbl_network_requests.request_to
      WHERE tbl_network_requests.request_from = ${userId}`

      let countQuery = `SELECT COUNT(tbl_user_details.email_id) as total_referrals
      FROM tbl_network_requests
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_network_requests.request_to
      LEFT JOIN tbl_user ON tbl_user.id = tbl_network_requests.request_to
      WHERE tbl_network_requests.request_from = ${userId}`
      if(search){
        query += ` AND tbl_user_details.company_name LIKE '%${search}%'`
        countQuery += ` AND tbl_user_details.company_name LIKE '%${search}%'`
      }
      query += ' ORDER BY tbl_network_requests.created_at DESC'

      if(resultPerPage && currentPage){
        var perPageString = ` LIMIT ${resultPerPage} OFFSET ${(currentPage - 1) * resultPerPage}`;
        query += perPageString
      }  
      const dbRes = await call({query},'makeQuery','get')
      const dbResCount = await call({query:countQuery},'makeQuery','get')

      resolve({
        success:true,
        message:{
          message:dbRes.message,
          total_count: dbResCount.message[0]?.total_referrals
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

exports.updateCompanyDocs = async(req,res) => {
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
    console.log('dbiinserttarrrrr',docIdArray)
    const query = `UPDATE tbl_user_details_extra SET doc_array = '${docIdArray.length ? docIdArray.toString() : null}' WHERE tbl_user_id=${reqBody.userId}`
    await dbPool.query(query)
    res.send({
      success:true,
      message:'Company details updated succesfully.'
    })
  }catch(e){
    console.log('error in updateDoc',e)
    res.send({
      success:false,
      message:'Failed to update company details.'
    })
  }
}


exports.getChannelPartnerListForAdmin = async (req,res) => {
  try{
    const result = await getChannelPartnerListForAdminFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getChannelPartnerListForAdminFunc = ({resultPerPage,currentPage,search,type_id, status,onlyShowForUserId,exporterName,contactNumber,contactPerson,leadAssignedTo,companyCity,sortCompanyName,sortContactPerson,sortCompanyCity,sortLeadAssignedTo,sortByDate}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let extraSearchQry = ""
      let todayDateObj = moment()
      let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
      if(status){
        let showActive = status.includes("Active")
        let showInactive = status.includes("Inactive")
        if(showActive && !showInactive){
          extraSearchQry += ` AND tbl_user.last_login_at >= DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) `
        }
        if(!showActive && showInactive){
          extraSearchQry += ` AND (tbl_user.last_login_at IS NULL OR tbl_user.last_login_at  < DATE_SUB(NOW(), INTERVAL ${activeUserLogicDurationInWeeks} WEEK) `
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
        extraSearchQry += ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.LeadAssignedTo IS NULL)`
      }
      let query = `SELECT 
      tbl_user.*, 
      tbl_user_details.company_name, 
      tbl_user_details.contact_person, 
      tbl_user_details.contact_number, 
      tbl_user_details.name_title, 
      tbl_user_details.phone_code, 
      tbl_user_details.country_code, 
      tbl_user_details.email_id, 
      tbl_user_details.company_city as company_city, 
      subAdminTblUserDetails.company_name as TaskAssignedToName,
      COUNT(tbl_network_requests.id) as noofreferral,
      tbl_user_refercode.refercode,
      tbl_user_tasks_logs.LOG_TYPE AS LastEventType,
      tbl_user_tasks_logs.CREATED_AT AS LastEventTime,
      tbl_user_tasks_logs.REMARK AS LastNote
    FROM 
      tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_user_tasks_logs ON tbl_user_tasks_logs.EXPORTER_CODE = tbl_user.id
      LEFT JOIN tbl_user_details subAdminTblUserDetails ON
      tbl_user.LeadAssignedTo = subAdminTblUserDetails.tbl_user_id
      
      LEFT JOIN tbl_user_refercode ON  tbl_user_refercode.tbl_user_id = tbl_user.id
      LEFT JOIN tbl_network_requests ON tbl_network_requests.request_from = tbl_user.id	
    WHERE 
      tbl_user.type_id = ${type_id} ${extraSearchQry}`

      let countQuery = `SELECT COUNT(tbl_user.id) as total_exporters FROM tbl_user 
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id  WHERE tbl_user.type_id = ${type_id} ${extraSearchQry} `
      if(search){
        query += ` AND (tbl_user_details.company_name LIKE '%${search}%' OR tbl_user_details.contact_number LIKE '%${search}%' OR tbl_user_details.contact_person LIKE '%${search}%')`
        countQuery += ` AND (tbl_user_details.company_name LIKE '%${search}%' OR tbl_user_details.contact_number LIKE '%${search}%' OR tbl_user_details.contact_person LIKE '%${search}%')`
      }
      query += `  GROUP BY tbl_user.id   `
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
      const dbRes = await call({query},'makeQuery','get')
      const dbResCount = await call({query:countQuery},'makeQuery','get')


      resolve({
        success:true,
        message:{
          message:dbRes.message,
          total_count: dbResCount.message[0]?.total_exporters
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

exports.getRefferalsListByCP = async (req,res) => {
  try{
    const result = await getRefferalsListByCPFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getRefferalsListByCPFunc = ({resultPerPage,currentPage,search,userId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let query = `
      SELECT 
        tbl_user.type_id,
        tbl_user_details.company_name,
        tbl_user_details.name_title,
        tbl_user_details.contact_person,
        tbl_user_details.contact_number,
        tbl_user_details.phone_code,
        tbl_user_details.company_country,
        o.*
        FROM   tbl_network_requests
          LEFT JOIN tbl_user_details
             ON tbl_user_details.tbl_user_id = tbl_network_requests.request_to
          LEFT JOIN tbl_user
             ON tbl_user.id = tbl_network_requests.request_to  
          LEFT JOIN (
            SELECT 
              notification_type, 
              notification_sub_type, 
              notification_description, 
              createdby, 
              refid 
            FROM 
              tbl_notification_logs 
            GROUP BY 
              createdby 
            limit 
              1
          ) o ON tbl_user.id = o.createdby        
      WHERE  tbl_network_requests.request_from = ${userId}`

      let countQuery = `SELECT COUNT(tbl_user_details.email_id) as total_referrals
      FROM tbl_network_requests
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_network_requests.request_to
      LEFT JOIN tbl_user ON tbl_user.id = tbl_network_requests.request_to
      WHERE tbl_network_requests.request_from = ${userId}`
      if(search){
        query += ` AND tbl_user_details.company_name LIKE '%${search}%'`
        countQuery += ` AND tbl_user_details.company_name LIKE '%${search}%'`
      }
      query += ' ORDER BY tbl_network_requests.created_at DESC'

      if(resultPerPage && currentPage){
        var perPageString = ` LIMIT ${resultPerPage} OFFSET ${(currentPage - 1) * resultPerPage}`;
        query += perPageString
      }  
      const dbRes = await call({query},'makeQuery','get')
      const dbResCount = await call({query:countQuery},'makeQuery','get')

      resolve({
        success:true,
        message:{
          message:dbRes.message,
          total_count: dbResCount.message[0]?.total_referrals
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

exports.getBranchesData = async (req,res) => {
  try{
    const result = await IECDetails.aggregate([
      {
          '$match': {
              'userId': req.body.userId
          }
      }, {
          '$project': {
              'branches': 1
          }
      }
  ])
    if(result?.[0]?.branches){
      res.send({
        success:true,
        message:result?.[0]?.branches
      })
    }else{
      res.send({
        success:false,
        message:[]
      })
    }
   
  }catch(e){
    console.log('error in api',e);
    res.send({
      success:false,
      message:e
    })
  }
}
 
exports.updateUserOnboardTask = async (req,res) => {
  try{
    const result =await updateUserOnboardTaskFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const updateUserOnboardTaskFunc = (reqBody) => {
  return new Promise(async(resolve,reject) => {
    try{
        const sqlFields = [];
        const sqlValues = [];
        const keys = Object.keys(reqBody)
        for(let i=0; i<=keys.length - 1 ; i++){
          let element = keys[i]
          if(reqBody[element]){
            sqlFields.push(element);
            if(element === 'EVENT_TIME'){
              sqlValues.push(moment(reqBody[element]).format('YYYY-MM-DD HH:MM:SS'));
            }else{
              sqlValues.push(typeof(reqBody[element])  === "string" ? mysqlTextParse(reqBody[element]) : reqBody[element]);
            }
          }
        }
        const fieldsString = sqlFields.join(",");
        const placeholders = sqlValues.join("','")
        const sql = `INSERT INTO tbl_user_tasks_logs (${fieldsString}) VALUES ('${placeholders}')`;

        await dbPool.query(sql)
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

exports.getUserOnboardedHistory = async(req,res) => {
  try{
    const {EXPORTER_CODE} = req.body
    if(EXPORTER_CODE){
      const query = `SELECT * FROM tbl_user_tasks_logs WHERE EXPORTER_CODE = '${EXPORTER_CODE}'`
      const dbRes = await call({query},'makeQuery','get')
      res.send({
        success:true,
        message:dbRes.message
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


exports.updateEnquiryTask = async (req,res) => {
  try{
    const result =await updateEnquiryTaskFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const updateEnquiryTaskFunc = (reqBody) => {
  return new Promise(async(resolve,reject) => {
    try{
        const sqlFields = [];
        const sqlValues = [];
        const keys = Object.keys(reqBody)
        for(let i=0; i<=keys.length - 1 ; i++){
          let element = keys[i]
          if(reqBody[element]){
            sqlFields.push(element);
            if(element === 'EVENT_TIME'){
              sqlValues.push(moment(reqBody[element]).format('YYYY-MM-DD HH:MM:SS'));
            }else{
              sqlValues.push(reqBody[element]);
            }
          }
        }
        const fieldsString = sqlFields.join(",");
        const placeholders = sqlValues.join("','");
        if(reqBody.LOG_TYPE === 'Lead Created'){
          let query = `UPDATE tbl_inquiry_from_website SET status=1 WHERE id = ${reqBody.EXPORTER_CODE}`
          await dbPool.query(query)
        }

        if(reqBody.LOG_TYPE === 'Lead Lost'){
          let query = `UPDATE tbl_inquiry_from_website SET status=2 WHERE id = ${reqBody.EXPORTER_CODE}`
          await dbPool.query(query)
        }

        const sql = `INSERT INTO tbl_enquiry_tasks_logs (${fieldsString}) VALUES ('${placeholders}')`;
        console.log('SQl Queryyyyyyy',sql);

        await dbPool.query(sql)
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

exports.getEnquiryHistory = async(req,res) => {
  try{
    const {EXPORTER_CODE} = req.body
    if(EXPORTER_CODE){
      const query = `SELECT * FROM tbl_enquiry_tasks_logs WHERE EXPORTER_CODE = '${EXPORTER_CODE}'`
      const dbRes = await call({query},'makeQuery','get')
      res.send({
        success:true,
        message:dbRes.message
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

exports.AssignUsersInBulkV2 = async (req,res) => {
  try{
    const {USER_IDS,LeadAssignedTo,SecondaryLeadAssignedTo} = req.body
    if(USER_IDS){
      let idsString = `('${USER_IDS.join("','")}')`
      const query = `UPDATE tbl_user SET LeadAssignedTo = '${LeadAssignedTo}', SecondaryLeadAssignedTo='${SecondaryLeadAssignedTo}' WHERE id IN ${idsString}`
      await dbPool.query(query)
      res.send({
        success:true,
        message:'Lead Assigned To updated Succesfully'
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

exports.getsubadminsByUser = async (req,res) => {
  try{
    const {userId} = req.body
    if(userId){
      const query = `
        SELECT LeadAssignedTo, SecondaryLeadAssignedTo FROM tbl_user WHERE id = '${userId}'
      `
      const  dbRes = await call({query}, 'makeQuery','get')
      const primaryAdmin = dbRes?.message?.[0]?.LeadAssignedTo || null
      let admminCnt = 0
      if(!isNaN(parseInt(primaryAdmin))){
        admminCnt += 1
      }
      const secondaryAdmin = dbRes?.message?.[0]?.SecondaryLeadAssignedTo || null
      if(!isNaN(parseInt(secondaryAdmin))){
        admminCnt += 1
      }
      console.log('dbRessssssss',dbRes,typeof(dbRes),primaryAdmin,typeof(secondaryAdmin),secondaryAdmin)

      let adminDetails = []
      if(admminCnt >= 1){
        for(let i =0; i<=admminCnt - 1;i++){
          const query = `SELECT tbl_user_id, contact_person FROM tbl_user_details WHERE tbl_user_id = '${i  === 0 ? primaryAdmin :secondaryAdmin}'`
          const  dbRes = await call({query}, 'makeQuery','get')
          const tasksQuery = `SELECT COUNT(id) as total_tasks FROM  tbl_user_tasks_logs WHERE CREATED_BY = '${dbRes?.message?.[0]?.tbl_user_id}' AND EXPORTER_CODE='${userId}'`
          const taskCountRes = await call({query: tasksQuery}, 'makeQuery','get')
          adminDetails.push({
            name : dbRes?.message?.[0]?.contact_person || "",
            id : dbRes?.message?.[0]?.tbl_user_id || "",
            admin_priority : i === 0 ? 'Primary' : 'Secondary',
            number_of_tasks: taskCountRes?.message?.[0]?.total_tasks || 0,
          })
        }
      }
     
      res.send({
        success:true,
        message:adminDetails
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

exports.getUserOnboardedHistoryAdminWise = async(req,res) => {
  try{
    const {EXPORTER_CODE,ADMIN_ID} = req.body
    if(EXPORTER_CODE){
      const query = `SELECT * FROM tbl_user_tasks_logs WHERE EXPORTER_CODE = '${EXPORTER_CODE}' AND CREATED_BY = '${ADMIN_ID}'`
      const dbRes = await call({query},'makeQuery','get')
      res.send({
        success:true,
        message:dbRes.message
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