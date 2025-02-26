const ExporterModelV2 = require("../../src/database/Models/ExporterModelV2");
const TTV = require("../../src/database/Models/TTVModel");
const { activeUserLogicDurationInWeeks, env } = require("../../urlCostants");
const { call } = require("../../utils/codeHelper")
const moment = require("moment");

exports.getReportsUserStats = async(req,res) => {
  try{
    const result = await getReportsUserStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getReportsUserStatsFunc = ({onlyShowForUserId,dateRangeQuery,from,to,userIds}) =>{
  return new Promise(async(resolve,reject) => {
    try{
      let todayDateObj = moment()
      let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
      let extraQuery = ''
      let dateRangeQuery = ''
      if(onlyShowForUserId){
        extraQuery = ` (AND tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(from && to){
        dateRangeQuery = ` AND tbl_user.created_at >= '${moment(from).format('YYYY-MM-DD')}' AND tbl_user.created_at <= '${moment(to).format('YYYY-MM-DD')}'`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}')`
      } 
      const countQuery = `
      SELECT 
        COUNT(*) as total_records,
        type_id,
        SUM(IF(last_login_at >= '${lastActiveDateStr}',1,0)) as active_users,
        SUM(IF(last_login_at < '${lastActiveDateStr}' OR last_login_at IS NULL,1,0)) as inactive_users
      FROM tbl_user 
      WHERE type_id IN ('8','19','20') ${extraQuery} ${dateRangeQuery} GROUP BY type_id` 

      const dbRes = await call({query:countQuery},'makeQuery','get')
      let mappingobj  = {
        8:'Fin',
        19:'Exp',
        20:'CP'
      }
      let respobj = {}
      let total_users = 0
      let total_active_users= 0
      let total_inactive_users = 0
      for(let i=0; i<=dbRes.message.length - 1 ; i++){
        const element = dbRes.message[i]
        total_users += element.total_records
        total_active_users +=  parseInt(element.active_users) 
        total_inactive_users +=  parseInt(element.inactive_users)
        respobj[`inactive_${mappingobj[element.type_id]}`] = element.inactive_users
        respobj[`active_${mappingobj[element.type_id]}`] = element.active_users
        respobj[`total_${mappingobj[element.type_id]}`] = element.total_records
      }
      respobj["total_users"] = total_users
      respobj["total_active_users"] = total_active_users
      respobj["total_inactive_users"] = total_inactive_users
      resolve({
        success:true,
        message:respobj
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
 
}

exports.getUserStatusGraph = async(req,res) => {
  try{
    const result = await getUserStatusGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getUserStatusGraphFunc = (reqBody) =>{
  return new Promise(async(resolve,reject) => {
    try{
      let todayDateObj = moment()
      let lastActiveDateStr = todayDateObj.clone().subtract(activeUserLogicDurationInWeeks, "weeks").format("YYYY-MM-DD")
      let barDataForUserOnboarded = []
      let tableDataForUserOnboarded = []
      let extraQuery = ''
      let extraQueryOnboard = ''
      let response = {}
      const {userIds} = reqBody
      if(reqBody.onlyShowForUserId){
        extraQuery = ` AND (tbl_user.LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}')`
        extraQueryOnboard = ` AND (LeadAssignedTo = '${reqBody.onlyShowForUserId}' OR SecondaryLeadAssignedTo = '${reqBody.onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}')`
        extraQueryOnboard = ` AND (LeadAssignedTo IN ('${userIds.join("','")}') OR SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`

      }
      let expSummary = { type: "Exporter", value: 0, active: 0, inActive: 0 }
      let importerSummary = { type: "Importer", value: 0, active: 0, inActive: 0 }
      let financierSummary = { type: "Financier", value: 0, active: 0, inActive: 0 }
      let channelPartnerSummary = { type: "Channel Partner", value: 0, active: 0, inActive: 0 }
  
      if (reqBody.from && reqBody.to) {
        // For exporters
        query = `SELECT * FROM tbl_user WHERE type_id = 19 AND created_at >= '${reqBody.from}' AND created_at <= '${reqBody.to}' ${extraQueryOnboard}`
        console.log('queryyyy',query);
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
        
  
        barDataForUserOnboarded.push(expSummary)
        //barDataForUserOnboarded.push(importerSummary)
        barDataForUserOnboarded.push(financierSummary)
        barDataForUserOnboarded.push(channelPartnerSummary)
  
        tableDataForUserOnboarded.push(["Active Users", expSummary["active"], financierSummary["active"], channelPartnerSummary["active"]])
        tableDataForUserOnboarded.push(["Inactive Users", expSummary["inActive"], financierSummary["inActive"], channelPartnerSummary["inActive"]])
  
        response["totalUsersOnboarded"] = expSummary["value"]  + financierSummary["value"] + channelPartnerSummary[["value"]]
        response["tableDataForUserOnboarded"] = tableDataForUserOnboarded
        response["barDataForUserOnboarded"] = barDataForUserOnboarded
      }
      resolve({
        success:true,
        message:response
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
 
}

exports.getUserOnboardgraph = async(req,res) => {
  try{
    const result = await getUserOnboardgraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getUserOnboardgraphFunc = ({from,to,userIds,onlyShowForUserId}) =>{
  return new Promise(async(resolve,reject) => {
    try{
      const todaysDate = new Date()
      const toDate = new Date(to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(to)
      let customerOnboardedData = []
      let countForMonths =  moment(to).diff(from,'month') + 1
      let dbRes 
      let finalRes = []
      let extraQuery = ''
      let extraQueryOnboard = ''

      if(onlyShowForUserId){
        extraQuery = ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
        extraQueryOnboard = ` AND (LeadAssignedTo = '${onlyShowForUserId}' OR SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND (tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`
        extraQueryOnboard = ` AND (LeadAssignedTo IN ('${userIds.join("','")}') OR SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`

      }
        // For Months
        if (countForMonths > 3) {
          for (let index = 0; index < countForMonths + 1; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
            let tempToDateObj = todayDateObj.clone().subtract(index, "months")
            let tempCustomerOnboarded = 0
            query = `SELECT id FROM tbl_user WHERE type_id = 19 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
          created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQuery}`
            dbRes = await call({ query }, 'makeQuery', 'get');
            tempCustomerOnboarded = dbRes.message.length
      
            let tempFinOnboarded = 0
            query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
            LEFT JOIN tbl_buyer_required_limit ON
            tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
            LEFT JOIN tbl_user ON 
            tbl_user.id = tbl_buyers_detail.user_id
            WHERE tbl_buyer_required_limit.id IS NOT NULL AND tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND  	tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}'${extraQueryOnboard} `
            let dbResFin = await call({ query }, 'makeQuery', 'get');
            tempFinOnboarded = dbResFin.message.length
      
            let tempCPOnboarded = 0
            query = `SELECT id FROM tbl_user WHERE type_id = 20 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
            created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQuery} `
            let dbResCP = await call({ query }, 'makeQuery', 'get');
            tempCPOnboarded = dbResCP.message.length
      
            customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), value: tempCustomerOnboarded, FinValue: tempFinOnboarded, CPValue: tempCPOnboarded })
          }
          finalRes = customerOnboardedData.reverse()
        }
        // For Days
        else if (countForMonths == 1) {
          countForMonths = moment(todayDateObj).clone().diff(from, "days")
          for (let index = 0; index < countForMonths + 1; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
            let tempToDateObj = todayDateObj.clone().subtract(index, "day")
            let tempCustomerOnboarded = 0
            query = `SELECT id FROM tbl_user WHERE type_id = 19 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
            created_at >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQuery}`
            dbRes = await call({ query }, 'makeQuery', 'get');
            tempCustomerOnboarded = dbRes.message.length
      
            let tempFinOnboarded = 0
            query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
            LEFT JOIN tbl_buyer_required_limit ON
            tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
            LEFT JOIN tbl_user ON 
            tbl_user.id = tbl_buyers_detail.user_id
            WHERE tbl_buyer_required_limit.id IS NOT NULL AND  	tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
            tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQueryOnboard}`
            let dbResFin = await call({ query }, 'makeQuery', 'get');
            tempFinOnboarded = dbResFin.message.length
      
            let tempCPOnboarded = 0
            query = `SELECT id FROM tbl_user WHERE type_id = 20 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
            created_at >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' ${extraQuery}`
            let dbResCP = await call({ query }, 'makeQuery', 'get');
            tempCPOnboarded = dbResCP.message.length
      
            customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"), value: tempCustomerOnboarded, FinValue: tempFinOnboarded, CPValue: tempCPOnboarded })
          }
          finalRes = customerOnboardedData.reverse()
        }
        // For Weeks
        else {
          countForMonths = moment(todayDateObj).clone().diff(to, "weeks")
          for (let index = 0; index < countForMonths + 1; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
            let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
            let tempCustomerOnboarded = 0
            query = `SELECT id FROM tbl_user WHERE type_id = 19 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
              created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQuery}`
            dbRes = await call({ query }, 'makeQuery', 'get');
            tempCustomerOnboarded = dbRes.message.length
      
            let tempFinOnboarded = 0
            query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
            LEFT JOIN tbl_buyer_required_limit ON
            tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
            LEFT JOIN tbl_user ON 
            tbl_user.id = tbl_buyers_detail.user_id
            WHERE tbl_buyer_required_limit.id IS NOT NULL AND 	tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
            tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQueryOnboard} `
            let dbResFin = await call({ query }, 'makeQuery', 'get');
            tempFinOnboarded = dbResFin.message.length
      
            let tempCPOnboarded = 0
            query = `SELECT id FROM tbl_user WHERE type_id = 20 AND created_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
              created_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}' ${extraQuery}`
            let dbResCP = await call({ query }, 'makeQuery', 'get');
            tempCPOnboarded = dbResCP.message.length
      
            customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, value: tempCustomerOnboarded, FinValue: tempFinOnboarded, CPValue: tempCPOnboarded })
          }
          finalRes = customerOnboardedData.reverse()
        }
      
      resolve({
        success:true,
        message:finalRes
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
 
}

exports.getAdminLCLimitGraph = async(req,res) => {
  try{
    const result = await getAdminLCLimitGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getAdminLCLimitGraphFunc = async({from,to,onlyShowForUserId,userIds}) => {
  return new Promise(async(resolve,reject) => {
    try{
      const todaysDate = new Date()
      const toDate = new Date(to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(to)
      let customerOnboardedData = []
      let countForMonths =  moment(to).diff(from,'month') + 1
      let dbRes 
      let finalRes = []
      let extraQuery = ''
      if(onlyShowForUserId){
        extraQuery = ` (AND tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}')`
      } 
        // For Months
      let lcSummary = {}
      if (countForMonths > 3) {
        for (let index = 0; index < countForMonths; index++) {
          // For LC
          // 0 pending
          // 1 Approved
          // 2 rejected
          // 3 Inprogress disbursement
          // 4 Disbursed
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
          let tempToDateObj = todayDateObj.clone().subtract(index, "months")
          let dateRangeQueryForLC = ` AND tbl_buyer_required_lc_limit.updatedAt <= '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND tbl_buyer_required_lc_limit.updatedAt > '${tempToDateObj.clone().format("YYYY-MM-01")}'  `

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
              WHERE 1 
              ${extraSearchQry} ${dateRangeQueryForLC} ${extraQuery}
              GROUP BY tbl_buyer_required_lc_limit.id
              HAVING ${havingSearchQry}`;
          let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          let sum = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element.ocrFields?.["32B2"]){
              sum += parseInt(element?.ocrFields?.["32B2"])
            }
          }
          lcSummary["pending"] = filterDbRes.message.length
          lcSummary["pendingAmount"] = sum

          // Approved
          extraSearchQry = ` (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 1 AND 
            tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 1) `
          filterQuery = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields
          
          FROM tbl_buyer_required_lc_limit
          WHERE ${extraSearchQry} ${dateRangeQueryForLC} ${extraQuery}`;
          filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          lcSummary["approved"] = filterDbRes.message.length
          let sum2 = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum2 += parseInt(element?.ocrFields?.["32B2"])
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
          WHERE 1 ${dateRangeQueryForLC} ${extraQuery}
          GROUP BY tbl_buyer_required_lc_limit.id 
          HAVING ${havingSearchQry}`;

          filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          lcSummary["rejected"] = filterDbRes.message.length
          let sum3 = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum3 += parseInt(element?.ocrFields?.["32B2"])
            }
          }
          lcSummary["rejectedAmount"] = sum3

          query = `SELECT id FROM tbl_buyer_required_lc_limit WHERE 1 ${dateRangeQueryForLC} `
          dbRes = await call({ query }, 'makeQuery', 'get');
          lcSummary["totalApplication"] = dbRes.message.length
      
          customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), ...lcSummary })
        }
        finalRes = customerOnboardedData.reverse()
      }
        // For Days
      else if (countForMonths <= 1) {
        countForMonths = moment(todayDateObj).clone().diff(from, "days")
        if(countForMonths === 0){
          countForMonths = 1
        }
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
          let tempToDateObj = todayDateObj.clone().subtract(index, "day")
          let dateRangeQueryForLC = ` AND tbl_buyer_required_lc_limit.updatedAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND tbl_buyer_required_lc_limit.updatedAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}'  `
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
              WHERE 1 
              ${extraSearchQry} ${dateRangeQueryForLC} ${extraQuery}
              GROUP BY tbl_buyer_required_lc_limit.id
              HAVING ${havingSearchQry}`;
          let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          let sum = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum += parseInt(element?.ocrFields?.["32B2"])
            }
          }
          lcSummary["pending"] = filterDbRes.message.length
          lcSummary["pendingAmount"] = sum

          // Approved
          extraSearchQry = ` (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 1 AND 
            tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 1) `
          filterQuery = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields
          
          FROM tbl_buyer_required_lc_limit
          WHERE ${extraSearchQry} ${dateRangeQueryForLC}`;
          filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          lcSummary["approved"] = filterDbRes.message.length
          let sum2 = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum2 += parseInt(element?.ocrFields?.["32B2"])
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
          WHERE 1 ${dateRangeQueryForLC} ${extraQuery}
          GROUP BY tbl_buyer_required_lc_limit.id 
          HAVING ${havingSearchQry}`;

          filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          lcSummary["rejected"] = filterDbRes.message.length
          let sum3 = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum3 += parseInt(element?.ocrFields?.["32B2"])
            }
          }
          lcSummary["rejectedAmount"] = sum3

          query = `SELECT id FROM tbl_buyer_required_lc_limit WHERE 1 ${dateRangeQueryForLC} `
          dbRes = await call({ query }, 'makeQuery', 'get');
          lcSummary["totalApplication"] = dbRes.message.length
      
          
          customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"), ...lcSummary})
        }
          finalRes = customerOnboardedData.reverse()
        }
        // For Weeks
        else {
          countForMonths = moment(todayDateObj).clone().diff(from, "weeks")   
          console.log('todaysssdataaa',todayDateObj);    
          for (let index = 0; index < countForMonths + 1; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
            let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
            let dateRangeQueryForLC = ` AND tbl_buyer_required_lc_limit.updatedAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND tbl_buyer_required_lc_limit.updatedAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}'  `
            console.log('dateRange',dateRangeQueryForLC);
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
              WHERE 1 
              ${extraSearchQry} ${dateRangeQueryForLC} ${extraQuery}
              GROUP BY tbl_buyer_required_lc_limit.id
              HAVING ${havingSearchQry}`;
          let filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          let sum = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum += parseInt(element?.ocrFields?.["32B2"])
            }
          }
          lcSummary["pending"] = filterDbRes.message.length
          lcSummary["pendingAmount"] = sum

          // Approved
          extraSearchQry = ` (tbl_buyer_required_lc_limit.contractDocsSignedByExporter = 1 AND 
            tbl_buyer_required_lc_limit.contractDocsSignedByFinancier = 1) `
          filterQuery = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields
          
          FROM tbl_buyer_required_lc_limit
          WHERE ${extraSearchQry} ${dateRangeQueryForLC}`;
          filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          lcSummary["approved"] = filterDbRes.message.length
          let sum2 = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum2 += parseInt(element?.ocrFields?.["32B2"])
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
          WHERE 1 ${dateRangeQueryForLC} ${extraQuery}
          GROUP BY tbl_buyer_required_lc_limit.id 
          HAVING ${havingSearchQry}`;

          filterDbRes = await call({ query: filterQuery }, 'makeQuery', 'get');
          lcSummary["rejected"] = filterDbRes.message.length
          let sum3 = 0
          for(let i = 0; i<= filterDbRes.message.length - 1; i++){
            const element = filterDbRes.message[i]
            if(element?.ocrFields?.["32B2"]){
              sum3 += parseInt(element?.ocrFields?.["32B2"])
            }
          }
          lcSummary["rejectedAmount"] = sum3

          query = `SELECT id FROM tbl_buyer_required_lc_limit WHERE 1 ${dateRangeQueryForLC} `
          dbRes = await call({ query }, 'makeQuery', 'get');
          lcSummary["totalApplication"] = dbRes.message.length
      
      
          customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, ...lcSummary })
        }
          finalRes = customerOnboardedData.reverse()
        }
      
      resolve({
        success:true,
        message:finalRes
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
}

exports.getAdminINVLimitGraph = async(req,res) => {
  try{
    const result = await getAdminINVLimitGraphFunc(req.body)
    res.send(result)
    
  }catch(e){
    res.send(e)
  }
}

const getAdminINVLimitGraphFunc = async({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      const todaysDate = new Date()
      const toDate = new Date(to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(to)
      let customerOnboardedData = []
      let countForMonths =  moment(to).diff(from,'month') + 1
      let dbRes 
      let finalRes = []
      let extraQuery = ''
      let extraQueryOnboard = ''

      if(onlyShowForUserId){
        extraQuery = ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
        extraQueryOnboard = ` AND (LeadAssignedTo = '${onlyShowForUserId}' OR SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND (tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`
        extraQueryOnboard = ` AND (LeadAssignedTo IN ('${userIds.join("','")}') OR SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`

      }
        // For Months
      let invSummary = {}
      if (countForMonths > 3) {
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
          let tempToDateObj = todayDateObj.clone().subtract(index, "months")
          let dateRangeQueryForInvoice = ` AND tbl_buyer_required_limit.updatedAt <= '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND tbl_buyer_required_limit.updatedAt > '${tempToDateObj.clone().format("YYYY-MM-01")}'  `

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
              WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
              ${extraSearchQryInv} ${dateRangeQueryForInvoice}
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
              WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
              ${extraSearchQryInv} ${dateRangeQueryForInvoice}
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
              WHERE tbl_buyer_required_limit.buyerId IS NOT NULL ${dateRangeQueryForInvoice}
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
      
          customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), ...invSummary})
        }
        finalRes = customerOnboardedData.reverse()
      }
        // For Days
      else if (countForMonths <= 1) {
        countForMonths = moment(todayDateObj).clone().diff(from, "days")
        if(countForMonths === 0){
          countForMonths = 1
        }
        for (let index = 0; index < countForMonths +  1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
          let tempToDateObj = todayDateObj.clone().subtract(index, "day")
          let dateRangeQueryForInvoice = ` AND tbl_buyer_required_limit.updatedAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND tbl_buyer_required_limit.updatedAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}'  `
  
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
              WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
              ${extraSearchQryInv} ${dateRangeQueryForInvoice}
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
              WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
              ${extraSearchQryInv} ${dateRangeQueryForInvoice}
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
              WHERE tbl_buyer_required_limit.buyerId IS NOT NULL ${dateRangeQueryForInvoice}
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
          customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"), ...invSummary})
        }
          finalRes = customerOnboardedData.reverse()
        }
        // For Weeks
        else {
          countForMonths = moment(todayDateObj).clone().diff(from, "weeks")
          console.log('countForMonths',countForMonths);

          for (let index = 0; index < countForMonths + 1 ; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
            let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
            let dateRangeQueryForInvoice = ` AND tbl_buyer_required_limit.updatedAt < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND tbl_buyer_required_limit.updatedAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}'  `


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
                WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
                ${extraSearchQryInv} ${dateRangeQueryForInvoice}
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
                WHERE tbl_buyer_required_limit.buyerId IS NOT NULL
                ${extraSearchQryInv} ${dateRangeQueryForInvoice}
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
                WHERE tbl_buyer_required_limit.buyerId IS NOT NULL ${dateRangeQueryForInvoice}
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

          customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, ...invSummary})
        }
          finalRes = customerOnboardedData.reverse()
        }
      
      resolve({
        success:true,
        message:finalRes
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
}

exports.getBuyersOnboardgraph = async(req,res) => {
  try{
    const result = await getBuyersOnboardgraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getBuyersOnboardgraphFunc = ({buyersAddedDuration}) =>{
  return new Promise(async(resolve,reject) => {
    try{
      let todayDateObj = moment()
      let customerOnboardedData = []
      let countForMonths = buyersAddedDuration?.split(" ")[0] / 1
      let dbRes 
      let finalRes = []
        // For Months
        if (countForMonths > 3) {
          for (let index = 0; index < countForMonths; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
            let tempToDateObj = todayDateObj.clone().subtract(index, "months")
            
            let buyersAdded = 0
            query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
            LEFT JOIN tbl_buyer_required_limit ON
            tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
            WHERE tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND  	tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' `
            let dbResFin = await call({ query }, 'makeQuery', 'get');
            buyersAdded = dbResFin.message.length
      
            customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), buyers: buyersAdded})
          }
          finalRes = customerOnboardedData.reverse()
        }
        // For Days
        else if (countForMonths == 1) {
          countForMonths = todayDateObj.clone().daysInMonth();
          for (let index = 0; index < countForMonths; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
            let tempToDateObj = todayDateObj.clone().subtract(index, "day")

      
            let buyersAdded = 0
            query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
            LEFT JOIN tbl_buyer_required_limit ON
            tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
            WHERE tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
            tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' `
            let dbResFin = await call({ query }, 'makeQuery', 'get');
            buyersAdded = dbResFin.message.length
  
            customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"), buyers:buyersAdded})
          }
          finalRes = customerOnboardedData.reverse()
        }
        // For Weeks
        else {
          const nextDate = todayDateObj.clone().add(countForMonths, "months");
          countForMonths = nextDate.clone().diff(todayDateObj, "weeks")
          for (let index = 0; index < countForMonths; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
            let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
    
            let buyersAdded = 0
            query = `SELECT tbl_buyer_required_limit.id FROM tbl_buyers_detail
            LEFT JOIN tbl_buyer_required_limit ON
            tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
            WHERE tbl_buyer_required_limit.createdAt  < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
            tbl_buyer_required_limit.createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' `
            let dbResFin = await call({ query }, 'makeQuery', 'get');
            buyersAdded = dbResFin.message.length
      
            customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`,buyers:buyersAdded})
          }
          finalRes = customerOnboardedData.reverse()
        }
      
      resolve({
        success:true,
        message:finalRes
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
 
}

exports.getBuyersStatusGraph = async(req,res) => {
  try{
    const result = await getBuyersStatusGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getBuyersStatusGraphFunc = ({}) =>{
  return new Promise(async(resolve,reject) => {
    try{
      const countQuery = `SELECT
      'Active Users' AS type,
      COUNT(DISTINCT tbl_buyers_detail.id) AS
  value
  FROM
      tbl_buyers_detail
  INNER JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
  UNION ALL
  SELECT
      'Inactive Users' AS type,
      COUNT(DISTINCT tbl_buyers_detail.id) AS
  value
  FROM
      tbl_buyers_detail
  LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
  WHERE
      tbl_buyer_required_limit.id IS NULL` 

      const dbRes = await call({query:countQuery},'makeQuery','get')
      resolve({
        success:true,
        message:dbRes.message
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
 
}

exports.getReportsBuyerList = async(req,res) => {
  try{
    const result = await getReportsBuyerListFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getReportsBuyerListFunc = ({currentPage,resultPerPage}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let query = `
        SELECT
        tbl_user_details.company_name,
        subAdminProfile.company_name AS SubAdminName,
        tbl_buyers_detail.buyerName,
        tbl_countries.name as buyerCountryName,
        COUNT(tbl_buyer_required_limit.id) AS limitCount,
        tbl_buyer_required_limit.buyers_credit,
        tbl_buyer_required_limit.termSheetSignedByExporter,
        tbl_buyer_required_limit.termSheetSignedByBank,
        tbl_buyers_detail.id AS buyerId,
        tbl_buyer_required_limit.id AS limitId,
        tbl_buyer_required_limit.updatedAt,
        JSON_EXTRACT(
            tbl_buyer_required_limit.selectedQuote,
            "$.financeLimit"
        ) AS availableLimit,
        tbl_buyer_required_limit.selectedFinancier,
        tbl_buyer_required_limit.invRefNo,
        tbl_buyer_required_limit.requiredLimit AS requiredLimit,
        tbl_user_details.tbl_user_id as userId
        FROM
            tbl_buyers_detail
        LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
        LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_buyers_detail.user_id
        LEFT JOIN tbl_user ON tbl_user.id = tbl_buyers_detail.user_id
        LEFT JOIN tbl_user_details subAdminProfile ON
            subAdminProfile.tbl_user_id = tbl_user.LeadAssignedTo
        LEFT JOIN tbl_countries ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
        GROUP BY tbl_buyers_detail.id ORDER BY tbl_buyer_required_limit.updatedAt DESC  
      `
      let Countquery = `
      SELECT
      COUNT(DISTINCT tbl_buyers_detail.id) as total_records
      FROM
          tbl_buyers_detail
      LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
      LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_buyers_detail.user_id
      LEFT JOIN tbl_user ON tbl_user.id = tbl_buyers_detail.user_id
      LEFT JOIN tbl_user_details subAdminProfile ON
          subAdminProfile.tbl_user_id = tbl_user.LeadAssignedTo
      LEFT JOIN tbl_countries ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry

      `
      if(resultPerPage && currentPage){
        var perPageString = ` LIMIT ${resultPerPage} OFFSET ${(currentPage - 1) * resultPerPage}`;
        query += perPageString
      } 
      const dbRes = await call({query},'makeQuery','get')
      const countRes = await call({query:Countquery},'makeQuery','get')
      resolve({
        success:true,
        message:{
          data: dbRes.message,
          total_records : countRes.message?.[0]?.total_records || 0
        }
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:e
      })
    }
  })
}

exports.getTaskAssignedGraph = async(req,res) => {
  try{
    const result = await getTaskAssignedGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getTaskAssignedGraphFunc = ({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths =  moment(to).diff(from,'month') 
      let dateFormat = ''
      if(countForMonths > 3){
        dateFormat = '%Y-%m-01'
      }else if(countForMonths <= 1){
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
            'TASK_DATE' :{
              $gte: new Date(from),
              $lte: new Date(to)
             }
          }
        }
      ]
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
          '$group': {
            '_id': {
              '$dateToString': {
                'format': dateFormat, 
                'date': '$TASK_DATE'
              }
            },
            'total_exporters':{$sum : 1}
          }
        }, {
          '$project': {
            '_id': 0, 
            'total_exporters':1,
            'xLabel': '$_id'
          }
        },{
          '$sort' :{
            'xLabel': 1
          }
      }]
      
      const response = await ExporterModelV2.aggregate(pipelinedata)
      resolve({
        success:true,
        message:response
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

exports.getTaskUpdateGraph = async(req,res) => {
  try{
    const result = await getTaskUpdateGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getTaskUpdateGraphFunc = ({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths =  moment(to).diff(from,'month') 
      let dateFormat = ''
      if(countForMonths > 3){
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
            'TASK_DATE' :{
              $gte: new Date(from),
              $lte: new Date(to)
             }
          }
        },
      ]
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
          '_id': null, 
          'not_interested': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    '$STATUS', 2
                  ]
                }, 1, 0
              ]
            }
          }, 
          'onboarded': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    '$STATUS', 4
                  ]
                }, 1, 0
              ]
            }
          }, 
          'lost': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    '$STATUS', 3
                  ]
                }, 1, 0
              ]
            }
          }, 
          'hot': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    {$first : '$task_logs.EVENT_STATUS'}, 'Hot (30 days or less)'
                  ]
                }, 1, 0
              ]
            }
          }, 
          'cold': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    {$first : '$task_logs.EVENT_STATUS'}, 'Cold (60 days or more)'
                  ]
                }, 1, 0
              ]
            }
          }, 
          'warm': {
            '$sum': {
              '$cond': [
                {
                  '$eq': [
                    {$first : '$task_logs.EVENT_STATUS'}, 'Warm (30-60 days)'
                  ]
                }, 1, 0
              ]
            }
          }, 
        }
      }, {
        '$project': {
          '_id': 0, 
          'not_interested': 1, 
          'onboarded': 1, 
          'lost': 1,
          'hot':1,
          'cold':1,
          'warm':1,
        }
      }]
      console.log('adsdassdasdas',JSON.stringify(pipelinedata));
      const response = await ExporterModelV2.aggregate(pipelinedata)
      const responseObj = response[0]
      const keys = Object.keys(responseObj)
      const values = Object.values(responseObj)
      let finaldata = []
      for(let i=0;i<=keys.length -1;i++){
        finaldata.push({
          label:keys[i],
          value: values[i]
        })
      }
      resolve({
        success:true,
        message:finaldata
      })
    }catch(e){
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getInboundTasks = async(req,res) => {
  try{
    const result = await getInboundTasksFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getInboundTasksFunc = ({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      const todaysDate = new Date()
      const toDate = new Date(to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(to)
      let customerOnboardedData = []
      let countForMonths =  moment(to).diff(from,'month')
      console.log('CountForMonths',countForMonths);
      let dbRes 
      let finalRes = []
        // For Months
        if (countForMonths > 3) {
          for (let index = 0; index < countForMonths; index++) {
            let tempFromDateObj 
            let tempToDateObj 
            if(index === 0){
              tempFromDateObj = moment(to).clone()
              tempToDateObj = moment(to).clone().subtract(index + 1, "months")
            }else{
              tempFromDateObj = moment(to).clone().subtract(index, "months")
              tempToDateObj = moment(to).clone().subtract(index + 1, "months")
            }
            let tempCustomerOnboarded = 0
            query = `SELECT id FROM  tbl_inquiry_from_website WHERE createdAt < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
            createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' `
            dbRes = await call({ query }, 'makeQuery', 'get');
            tempCustomerOnboarded = dbRes.message.length
      
            customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), value: tempCustomerOnboarded })
          }
          finalRes = customerOnboardedData.reverse()
        }
        // For Days
        else if (countForMonths <= 1) {
          countForMonths = moment(todayDateObj).clone().diff(from, "days")
          if(countForMonths === 0){
            countForMonths = 1
          }
          for (let index = 0; index < countForMonths + 1; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
            let tempToDateObj = todayDateObj.clone().subtract(index, "day")
            let tempCustomerOnboarded = 0
            query = `SELECT id FROM tbl_inquiry_from_website WHERE createdAt < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND
            createdAt >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' `
            dbRes = await call({ query }, 'makeQuery', 'get');
            tempCustomerOnboarded = dbRes.message.length
    
            customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"), value: tempCustomerOnboarded})
          }
          finalRes = customerOnboardedData.reverse()
        }
        // For Weeks
        else {
          countForMonths = moment(todayDateObj).clone().diff(from, "weeks")
          for (let index = 0; index < countForMonths; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
            let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
            let tempCustomerOnboarded = 0
            query = `SELECT id FROM tbl_inquiry_from_website WHERE createdAt < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND
            createdAt >= '${tempToDateObj.clone().format("YYYY-MM-01")}' `
            dbRes = await call({ query }, 'makeQuery', 'get');
            tempCustomerOnboarded = dbRes.message.length
      
            customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, value: tempCustomerOnboarded })
          }
          finalRes = customerOnboardedData.reverse()
        }
        return resolve({
          success:true,
          message:finalRes
        })
    }catch(e){
      return reject({
        success:false,
        message:e
      })
    }
  })  
}

exports.getApprovedBuyers = async(req,res) => {
  try{
    const result = await getApprovedBuyersFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getApprovedBuyersFunc = ({}) => {
  return new Promise(async(resolve,reject) => {
    try{
        const query = `SELECT
        tbl_buyers_detail.buyerName as type,
        CAST(JSON_EXTRACT(tbl_buyer_required_limit.selectedQuote, "$.financeLimit") AS DECIMAL(10,2)) AS value
    FROM
        tbl_buyers_detail
    LEFT JOIN
        tbl_buyer_required_limit ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
        JSON_EXTRACT(tbl_buyer_required_limit.selectedQuote, "$.financeLimit") IS NOT NULL
    GROUP BY tbl_buyers_detail.id
    ORDER BY
        CAST(JSON_EXTRACT(tbl_buyer_required_limit.selectedQuote, "$.financeLimit") AS DECIMAL(10,2)) DESC
    LIMIT 10;`
    
    const dbRes = await call({query},'makeQuery','get')

    return resolve({
      success:true,
      message:dbRes.message
    })
    }catch(e){
      return reject({
        success:false,
        message:e
      })
    }
  })  
}

exports.getTopLimitBuyers = async(req,res) => {
  try{
    const result = await getTopLimitBuyersFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getTopLimitBuyersFunc = ({}) => {
  return new Promise(async(resolve,reject) => {
    try{
        const query = `SELECT
        tbl_buyers_detail.buyerName as type,
        tbl_buyer_required_limit.requiredLimit as value
    FROM
        tbl_buyers_detail
    LEFT JOIN
        tbl_buyer_required_limit ON tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
        tbl_buyer_required_limit.requiredLimit IS NOT NULL
    GROUP BY tbl_buyers_detail.id
    ORDER BY
        tbl_buyer_required_limit.requiredLimit DESC
    LIMIT 10;`
    
    const dbRes = await call({query},'makeQuery','get')

    return resolve({
      success:true,
      message:dbRes.message
    })
    }catch(e){
      return reject({
        success:false,
        message:e
      })
    }
  })  
}

exports.getTopBuyersByCountry = async(req,res) => {
  try{
    const result = await getTopBuyersByCountryFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getTopBuyersByCountryFunc = ({}) => {
  return new Promise(async(resolve,reject) => {
    try{
        const query = `SELECT COUNT(tbl_buyers_detail.id) as value,tbl_countries.name as type FROM tbl_buyers_detail 
        LEFT JOIN tbl_countries ON tbl_countries.sortname = tbl_buyers_detail.buyerCountry
        GROUP BY tbl_buyers_detail.buyerCountry ORDER BY COUNT(tbl_buyers_detail.id) DESC LIMIT 10;`
    
    const dbRes = await call({query},'makeQuery','get')

    return resolve({
      success:true,
      message:dbRes.message
    })
    }catch(e){
      return reject({
        success:false,
        message:e
      })
    }
  })  
}


exports.getGeographicaldata = async(req,res) => {
  try{
    const result = await getGeographicaldataFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getGeographicaldataFunc = ({userIds,from,to,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let extraQuery = ''
      let dateRangeQuery = ''
      if(onlyShowForUserId){
        extraQuery = ` (AND tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(from && to){
        dateRangeQuery = ` AND tbl_user.created_at >= '${moment(from).format('YYYY-MM-DD')}' AND tbl_user.created_at <= '${moment(to).format('YYYY-MM-DD')}'`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}')`
      } 
      const query = `SELECT
            tbl_user_details.company_city,
            SUM(CASE WHEN tbl_user.type_id = 8 THEN 1 ELSE 0 END) AS financer_count,
            SUM(CASE WHEN tbl_user.type_id = 19 THEN 1 ELSE 0 END) AS exporter_count,
            SUM(CASE WHEN tbl_user.type_id = 20 THEN 1 ELSE 0 END) AS cp_count,
            COUNT(tbl_user.id) AS total_count,
            SUM(CASE WHEN tbl_user.type_id IN (8, 19, 20) THEN 1 ELSE 0 END) AS sum_count
        FROM
            tbl_user
            LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
            WHERE tbl_user.type_id IN (8, 19, 20) ${extraQuery} ${dateRangeQuery}
            GROUP BY
              tbl_user_details.company_city
            ORDER BY
              sum_count DESC LIMIT 10`
    
      const dbRes = await call({query},'makeQuery','get')

      return resolve({
        success:true,
        message:dbRes.message
      })
    }catch(e){
      return reject({
        success:false,
        message:e
      })
    }
  })  
}

exports.getExportWisedata = async(req,res) => {
  try{
    const result = await getExportWisedataFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getExportWisedataFunc = ({from,to,groupParam}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let dbRes
      if(groupParam === 'Lanes'){
        dbRes = await TTV.aggregate([
          {
            $match: {
              'DATE' : {
                $gte : new Date(from),
                $lte : new Date(to)
              }
            },
          },
          {
            $group: {
              '_id': {
                source: '$INDIAN_PORT',
                destination:'$DESTINATION_PORT'
              },
              'FOB': {
                $sum: '$FOB_VALUE_USD'
              },
              'SHIPMENTS':{
                $sum: 1
              }
            }
          },{
            $project: {
              _id: 0,
              FOB:1,
              SHIPMENTS:1,
              source:'$_id.source',
              destination:'$_id.destination'
            }
          },
          {
            $sort : {
              'FOB' : -1
            }
          },
          {
            $limit:10
        }])
      }else{
        dbRes = await TTV.aggregate([
          {
            $match: {
              'DATE' : {
                $gte : new Date(from),
                $lte : new Date(to)
              }
            },
          },
          {
            $group: {
              '_id': groupParam === 'HS_CODE' ? {$substr: ['$HS_CODE',0,2]} : `$${groupParam}`,
              'FOB': {
                $sum: '$FOB_VALUE_USD'
              },
              'SHIPMENTS':{
                $sum: 1
              }
            }
          },{
            $project: {
              _id: 0,
              FOB:1,
              SHIPMENTS:1,
              label:'$_id'
            }
          },
          {
            $sort : {
              'FOB' : -1
            }
          },
          {
            $limit:10
        }])
      }
      return resolve({
        success:true,
        message:dbRes
      })
    }catch(e){
      console.log('error in getExportWisedata');
      return reject({
        success:false,
        message:e
      })
    }
  })  
}

exports.getBuyersByExporters = async(req,res) => {
  try{
    const result = await getBuyersByExportersFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getBuyersByExportersFunc = ({onlyShowForUserId,from,to,userIds}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let extraQuery = ''
      let dateRangeQuery = ''
      if(onlyShowForUserId){
        extraQuery = ` (AND tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(from && to){
        dateRangeQuery = ` AND tbl_buyers_detail.created_at >= '${moment(from).format('YYYY-MM-DD')}' AND tbl_buyers_detail.created_at <= '${moment(to).format('YYYY-MM-DD')}'`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}')`
      } 

      const query = `SELECT
      COUNT(tbl_buyers_detail.id) AS count,
      tbl_user_details.company_name AS label
  FROM
      tbl_buyers_detail
  LEFT JOIN tbl_user_details ON
    tbl_user_details.tbl_user_id = tbl_buyers_detail.user_id
  LEFT JOIN tbl_user ON
   tbl_user.id =  tbl_buyers_detail.user_id
  WHERE 1 ${extraQuery} ${dateRangeQuery}
  GROUP BY
      tbl_buyers_detail.user_id
  ORDER BY
  count
  DESC LIMIT 10;`
    
      const dbRes = await call({query},'makeQuery','get')

      return resolve({
        success:true,
        message:dbRes.message
      })
    }catch(e){
      console.log('error in e',e);
      return reject({
        success:false,
        message:e
      })
    }
  })  
}

exports.getShipmentsdata = async(req,res) => {
  try{
    const result = await getShipmentsdataFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}
const getShipmentsdataFunc = ({from,to,groupParam}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let countForMonths =  moment(to).diff(from,'month') 
      let dateFormat = ''
      if(countForMonths > 3){
        dateFormat = '%Y-%m-01'
      }else if(countForMonths <= 1){
        dateFormat = '%Y-%m-%d'
      }else{
        dateFormat = "W%V"
      }
      let dbRes = await TTV.aggregate([
        {
          $match: {
            'DATE' : {
              $gte : new Date(from),
              $lte : new Date(to)
            }
          },
        },
        {
          $group: {
            '_id': {
              '$dateToString': {
                'format': dateFormat, 
                'date': '$DATE'
              }
            },
            'FOB': {
              $sum: '$FOB_VALUE_USD'
            },
            'SHIPMENTS':{
              $sum: 1
            }
          }
        },{
          $project: {
            _id: 0,
            FOB:1,
            SHIPMENTS:1,
            label:'$_id'
          }
        },
        {
          $sort : {
            'label' : 1
          }
        },
        {
          $limit:10
      }])
      return resolve({
        success:true,
        message:dbRes
      })
    }catch(e){
      console.log('error in getExportWisedata');
      return reject({
        success:false,
        message:e
      })
    }
  })  
}

exports.getDiscountingGraph =  async (req,res) => {
  try{
    const result = await getDiscountingGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getDiscountingGraphFunc = ({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      const todaysDate = new Date()
      const toDate = new Date(to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(to)
      let customerOnboardedData = []
      let countForMonths =  moment(to).diff(from,'month') + 1
      let finalRes = []
      let extraQuery = ''

      if(onlyShowForUserId){
        extraQuery = ` AND (LeadAssignedTo = '${onlyShowForUserId}' OR SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND (LeadAssignedTo IN ('${userIds.join("','")}') OR SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`
      }
        // For Months
      if (countForMonths > 3) {
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
          let tempToDateObj = todayDateObj.clone().subtract(index, "months")
          let dateRangeQuery = ` AND tbl_disbursement_scheduled.scheduledOn >= '${tempToDateObj.clone().format("YYYY-MM-01")}' AND tbl_disbursement_scheduled.scheduledOn <= '${tempFromDateObj.clone().format("YYYY-MM-01")}'  `
          let disCountingQuery = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDisbursed FROM tbl_disbursement_scheduled
          LEFT JOIN tbl_invoice_discounting ON 
          tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
          LEFT JOIN tbl_buyer_required_lc_limit ON
          tbl_buyer_required_lc_limit.id = tbl_disbursement_scheduled.invRefNo
          LEFT JOIN tbl_user ON
          tbl_user.id = COALESCE(tbl_invoice_discounting.seller_id, tbl_buyer_required_lc_limit.createdBy)
          WHERE tbl_disbursement_scheduled.status = 1 ${dateRangeQuery} ${extraQuery}`;
          let disCountingRes = await call({ query: disCountingQuery }, 'makeQuery', 'get');

          console.log('Queryyyyyyy', disCountingQuery)

          customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), discounting : disCountingRes?.message?.[0]?.totalDisbursed || 0  })
        }
        finalRes = customerOnboardedData.reverse()
      }
        // For Days
      else if (countForMonths <= 1) {
        countForMonths = moment(todayDateObj).clone().diff(from, "days")
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
          let tempToDateObj = todayDateObj.clone().subtract(index, "day")

          let dateRangeQuery = ` AND tbl_disbursement_scheduled.scheduledOn >= '${tempToDateObj.clone().format("YYYY-MM-DD")}' AND tbl_disbursement_scheduled.scheduledOn < '${tempFromDateObj.clone().format("YYYY-MM-DD")}'  `

          let disCountingQuery = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDisbursed FROM tbl_disbursement_scheduled
          LEFT JOIN tbl_invoice_discounting ON 
          tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
          LEFT JOIN tbl_buyer_required_lc_limit ON
          tbl_buyer_required_lc_limit.id = tbl_disbursement_scheduled.invRefNo
          LEFT JOIN tbl_user ON
          tbl_user.id = COALESCE(tbl_invoice_discounting.seller_id, tbl_buyer_required_lc_limit.createdBy)
          WHERE tbl_disbursement_scheduled.status = 1 ${dateRangeQuery} ${extraQuery}`;
          let disCountingRes = await call({ query: disCountingQuery }, 'makeQuery', 'get');

          customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"),  discounting : disCountingRes?.message?.[0]?.totalDisbursed || 0 })
        }
          finalRes = customerOnboardedData.reverse()
        }
        // For Weeks
        else {
          countForMonths = moment(todayDateObj).clone().diff(to, "weeks")
          for (let index = 0; index < countForMonths + 1; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
            let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
            let dateRangeQuery = ` AND tbl_disbursement_scheduled.scheduledOn >= '${tempToDateObj.clone().format("YYYY-MM-01")}' AND tbl_disbursement_scheduled.scheduledOn <= '${tempFromDateObj.clone().format("YYYY-MM-01")}'  `
            let extraQuery =  ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}')`
            let disCountingQuery = `SELECT SUM(tbl_disbursement_scheduled.amountInUSD) AS totalDisbursed FROM tbl_disbursement_scheduled
            LEFT JOIN tbl_invoice_discounting ON 
            tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
            LEFT JOIN tbl_buyer_required_lc_limit ON
            tbl_buyer_required_lc_limit.id = tbl_disbursement_scheduled.invRefNo
            LEFT JOIN tbl_user ON
            tbl_user.id = COALESCE(tbl_invoice_discounting.seller_id, tbl_buyer_required_lc_limit.createdBy)
            WHERE tbl_disbursement_scheduled.status = 1 ${dateRangeQuery} ${extraQuery}`;
            let disCountingRes = await call({ query: disCountingQuery }, 'makeQuery', 'get');
  
        

          customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`,  discounting : disCountingRes?.message?.[0]?.totalDisbursed || 0 })
        }
          finalRes = customerOnboardedData.reverse()
        }
      
      resolve({
        success:true,
        message:finalRes
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
}


exports.getAdminINVFinGraph = async(req,res) => {
  try{
    const result = await getAdminINVFinGraphFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getAdminINVFinGraphFunc = async({from,to,userIds,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      const todaysDate = new Date()
      const toDate = new Date(to)
      let todayDateObj = toDate > todaysDate ? moment(): moment(to)
      let customerOnboardedData = []
      let countForMonths =  moment(to).diff(from,'month') + 1
      let dbRes 
      let finalRes = []
      let extraQuery = ''

      if(onlyShowForUserId){
        extraQuery = ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND (tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`
      }
        // For Months
      let invSummary = {}
      if (countForMonths > 3) {
        for (let index = 0; index < countForMonths; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "months")
          let tempToDateObj = todayDateObj.clone().subtract(index, "months")
          let dateRangeQueryForInvoice = ` AND tbl_invoice_discounting.modified_at <= '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND tbl_invoice_discounting.modified_at > '${tempToDateObj.clone().format("YYYY-MM-01")}'  `
          
          let filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
          tbl_invoice_discounting.status != 6 ${dateRangeQueryForInvoice} ${extraQuery}`;
          let filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
          invSummary["pending"] = filterDbResInv.message.length
          let suminvpending = 0
          for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
            const element = filterDbResInv.message[i]
            if(element.amount){
              suminvpending += parseInt(element.amount)
            }
          }  
          invSummary["pendingAmount"] = suminvpending
          // Approved
         
          filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
          WHERE (tbl_invoice_discounting.status = 3 OR tbl_invoice_discounting.status = 4 OR tbl_invoice_discounting.status = 6) ${dateRangeQueryForInvoice} ${extraQuery}`;
          filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
          invSummary["approved"] = filterDbResInv.message.length
          let suminvapp = 0
          for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
            const element = filterDbResInv.message[i]
            if(element.amount){
              suminvapp += parseInt(element.amount)
            }
          }  
          invSummary["approvedAmount"] = suminvapp
          // Rejected
          filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
          WHERE tbl_invoice_discounting.status = 5 ${dateRangeQueryForInvoice} ${extraQuery}`;
    
          filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
          invSummary["rejected"] = filterDbResInv.message.length
          let suminvrej = 0
          for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
            const element = filterDbResInv.message[i]
            if(element.amount){
              suminvrej += parseInt(element.amount)
            }
          }  
          invSummary["rejectedAmount"] = suminvrej
      
          customerOnboardedData.push({ label: tempToDateObj.clone().format("MMM YYYY"), ...invSummary})
        }
        finalRes = customerOnboardedData.reverse()
      }
        // For Days
      else if (countForMonths == 1) {
        countForMonths = moment(todayDateObj).clone().diff(from, "days")
        if(countForMonths === 0){
          countForMonths = 1
        }
        for (let index = 0; index < countForMonths + 1; index++) {
          let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "day")
          let tempToDateObj = todayDateObj.clone().subtract(index, "day")
          let dateRangeQueryForInvoice = ` AND tbl_invoice_discounting.modified_at < '${tempFromDateObj.clone().format("YYYY-MM-DD")}' AND tbl_invoice_discounting.modified_at >= '${tempToDateObj.clone().format("YYYY-MM-DD")}'  `
  
          let filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
          tbl_invoice_discounting.status != 6 ${dateRangeQueryForInvoice} ${extraQuery}`;
          let filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
          invSummary["pending"] = filterDbResInv.message.length
          let suminvpending = 0
          for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
            const element = filterDbResInv.message[i]
            if(element.amount){
              suminvpending += parseInt(element.amount)
            }
          }  
          invSummary["pendingAmount"] = suminvpending
          // Approved
          filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
          WHERE tbl_invoice_discounting.status = 5 ${dateRangeQueryForInvoice} ${extraQuery}`;
          filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
          invSummary["approved"] = filterDbResInv.message.length
          let suminvapp = 0
          for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
            const element = filterDbResInv.message[i]
            if(element.amount){
              suminvapp += parseInt(element.amount)
            }
          }  
          invSummary["approvedAmount"] = suminvapp
          // Rejected
          filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
          WHERE tbl_invoice_discounting.status = 5 ${dateRangeQueryForInvoice} ${extraQuery}`;
    
          filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
          invSummary["rejected"] = filterDbResInv.message.length
          let suminvrej = 0
          for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
            const element = filterDbResInv.message[i]
            if(element.amount){
              suminvrej += parseInt(element.amount)
            }
          }  
          invSummary["rejectedAmount"] = suminvrej
          customerOnboardedData.push({ label: tempToDateObj.clone().format("DD MMM"),...invSummary})
        }
          finalRes = customerOnboardedData.reverse()
        }
        // For Weeks
        else {
          countForMonths = moment(todayDateObj).clone().diff(to, "weeks")
          for (let index = 0; index < countForMonths; index++) {
            let tempFromDateObj = todayDateObj.clone().subtract(index - 1, "weeks")
            let tempToDateObj = todayDateObj.clone().subtract(index, "weeks")
            let dateRangeQueryForInvoice = ` AND tbl_invoice_discounting.modified_at < '${tempFromDateObj.clone().format("YYYY-MM-01")}' AND tbl_invoice_discounting.modified_at >= '${tempToDateObj.clone().format("YYYY-MM-01")}'  `

            let filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
            tbl_invoice_discounting.status != 6 ${dateRangeQueryForInvoice} ${extraQuery}`;
            let filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
            invSummary["pending"] = filterDbResInv.message.length
            let suminvpending = 0
            for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
              const element = filterDbResInv.message[i]
              if(element.amount){
                suminvpending += parseInt(element.amount)
              }
            }  
            invSummary["pendingAmount"] = suminvpending
            // Approved
            filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
            WHERE tbl_invoice_discounting.status = 5 ${dateRangeQueryForInvoice} ${extraQuery}`;
            filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
            invSummary["approved"] = filterDbResInv.message.length
            let suminvapp = 0
            for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
              const element = filterDbResInv.message[i]
              if(element.amount){
                suminvapp += parseInt(element.amount)
              }
            }  
            invSummary["approvedAmount"] = suminvapp
            // Rejected
            filterQueryInv = `SELECT tbl_invoice_discounting.id AS invoice_discounting_table_id, tbl_invoice_discounting.amount as amount
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
            WHERE tbl_invoice_discounting.status = 5 ${dateRangeQueryForInvoice} ${extraQuery}`;
      
            filterDbResInv = await call({ query: filterQueryInv }, 'makeQuery', 'get');
            invSummary["rejected"] = filterDbResInv.message.length
            let suminvrej = 0
            for(let i = 0; i<= filterDbResInv.message.length - 1; i++){
              const element = filterDbResInv.message[i]
              if(element.amount){
                suminvrej += parseInt(element.amount)
              }
            }  
            invSummary["rejectedAmount"] = suminvrej

          customerOnboardedData.push({ label: `${tempToDateObj.clone().format("DD MMM")} -> ${tempFromDateObj.clone().format("DD MMM")}`, ...invSummary})
        }
          finalRes = customerOnboardedData.reverse()
        }
      
      resolve({
        success:true,
        message:finalRes
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{...e}
      })
    }
  })
}

exports.getDeals = async (req,res) => {
  try{
    const result = await getDealsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getDealsFunc = async({from,to,selectedTypeDealsGraph,userIds,onlyShowForUserId}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let dbRes = {
        success:true,
        message:[]
      }
      let extraQuery = ''
      if(onlyShowForUserId){
        extraQuery = ` (AND tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR  tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}')`
      } 
      if(selectedTypeDealsGraph === 'Exporter'){
        let query = `
        SELECT
          company_name,
          SUM(inv_value) AS total_inv_value,
          SUM(inv_count) AS total_inv_count,
          SUM(lc_value) AS total_lc_value,
          SUM(lc_count) AS total_lc_count
        FROM
        (
          SELECT
            tbl_user_details.company_name,
            SUM(
                tbl_buyer_required_limit.requiredLimit
            ) AS inv_value,
            COUNT(
                DISTINCT tbl_buyer_required_limit.id
            ) AS inv_count,
            0 AS lc_value,
            0 AS lc_count
          FROM
            tbl_user
        LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
        LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.userId = tbl_user.id
        WHERE
            tbl_user.type_id = '19' AND tbl_buyer_required_limit.limitPendingFrom IS NULL AND(
                tbl_buyer_required_limit.createdAt >= '${from}'AND tbl_buyer_required_limit.createdAt <= '${to}' ${extraQuery}
            )
        GROUP BY
            tbl_user_details.company_name
        UNION
        
        SELECT
          tbl_user_details.company_name,
          0 AS inv_value,
          0 AS inv_count,
          SUM(
            CASE WHEN JSON_VALID(
                tbl_buyer_required_lc_limit.ocrFields
            ) THEN JSON_UNQUOTE(
                JSON_EXTRACT(
                    tbl_buyer_required_lc_limit.ocrFields,
                    '$."32B2"'
                )
            ) ELSE 0
        END
        ) AS lc_value,
        COUNT(
        DISTINCT tbl_buyer_required_lc_limit.id
        ) AS lc_count
        FROM
          tbl_user
        LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
        LEFT JOIN tbl_buyer_required_lc_limit ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id
        WHERE
          tbl_user.type_id = '19' AND(
            tbl_buyer_required_lc_limit.createdAt >= '${from}' AND tbl_buyer_required_lc_limit.createdAt <= '${to}' ${extraQuery}
        )
        GROUP BY
          tbl_user_details.company_name
        ) AS combined_data
        GROUP BY
          company_name
        ORDER BY
        (
            total_lc_value + total_inv_value
        )
        DESC
        LIMIT 10
        `
        let response = await call({query}, 'makeQuery', 'get')  
        dbRes.message = response.message
      }
      if(selectedTypeDealsGraph === 'Financer'){
        let query = `
        SELECT
          company_name,
          SUM(inv_value) AS total_inv_value,
          SUM(inv_count) AS total_inv_count,
          SUM(lc_value) AS total_lc_value,
          SUM(lc_count) AS total_lc_count
        FROM
        (
          SELECT
            tbl_user_details.company_name,
            SUM(
                tbl_buyer_required_limit.requiredLimit
            ) AS inv_value,
            COUNT(
                DISTINCT tbl_buyer_required_limit.id
            ) AS inv_count,
            0 AS lc_value,
            0 AS lc_count
          FROM
            tbl_user
        LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
        LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.selectedFinancier = tbl_user.id
        WHERE
            tbl_user.type_id = '8' AND tbl_buyer_required_limit.limitPendingFrom IS NULL AND(
                tbl_buyer_required_limit.createdAt >= '${from}'AND tbl_buyer_required_limit.createdAt <= '${to}' ${extraQuery}
            )
        GROUP BY
            tbl_user_details.company_name
        UNION
        
        SELECT
          tbl_user_details.company_name,
          0 AS inv_value,
          0 AS inv_count,
          SUM(
            CASE WHEN JSON_VALID(
                tbl_buyer_required_lc_limit.ocrFields
            ) THEN JSON_UNQUOTE(
                JSON_EXTRACT(
                    tbl_buyer_required_lc_limit.ocrFields,
                    '$."32B2"'
                )
            ) ELSE 0
        END
        ) AS lc_value,
        COUNT(
        DISTINCT tbl_buyer_required_lc_limit.id
        ) AS lc_count
        FROM
          tbl_user
        LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
        LEFT JOIN tbl_buyer_required_lc_limit ON tbl_buyer_required_lc_limit.selectedFinancier = tbl_user.id
        WHERE
          tbl_user.type_id = '8' AND(
            tbl_buyer_required_lc_limit.createdAt >= '${from}' AND tbl_buyer_required_lc_limit.createdAt <= '${to}' ${extraQuery}
        )
        GROUP BY
          tbl_user_details.company_name
        ) AS combined_data
        GROUP BY
          company_name
        ORDER BY
        (
            total_lc_value + total_inv_value
        )
        DESC
        LIMIT 10
        `
        let response = await call({query}, 'makeQuery', 'get')  
        dbRes.message = response.message
      }
      if(selectedTypeDealsGraph === 'Partner'){
        let query = `
        SELECT
          company_name,
          SUM(inv_value) AS total_inv_value,
          SUM(inv_count) AS total_inv_count,
          SUM(lc_value) AS total_lc_value,
          SUM(lc_count) AS total_lc_count
        FROM
        (
          SELECT
            tbl_user_details.company_name,
            SUM(
                tbl_buyer_required_limit.requiredLimit
            ) AS inv_value,
            COUNT(
                DISTINCT tbl_buyer_required_limit.id
            ) AS inv_count,
            0 AS lc_value,
            0 AS lc_count
          FROM
            tbl_user

        LEFT JOIN tbl_network_requests ON tbl_user.id = tbl_network_requests.request_to
        LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_network_requests.request_from
        LEFT JOIN tbl_buyer_required_limit ON tbl_buyer_required_limit.userId = tbl_user.id
        WHERE
            tbl_user.type_id = '19' AND tbl_buyer_required_limit.limitPendingFrom IS NULL AND(
                tbl_buyer_required_limit.createdAt >= '${from}'AND tbl_buyer_required_limit.createdAt <= '${to}' ${extraQuery}
            )
        GROUP BY
            tbl_user_details.company_name
        UNION
        
        SELECT
          tbl_user_details.company_name,
          0 AS inv_value,
          0 AS inv_count,
          SUM(
            CASE WHEN JSON_VALID(
                tbl_buyer_required_lc_limit.ocrFields
            ) THEN JSON_UNQUOTE(
                JSON_EXTRACT(
                    tbl_buyer_required_lc_limit.ocrFields,
                    '$."32B2"'
                )
            ) ELSE 0
        END
        ) AS lc_value,
        COUNT(
        DISTINCT tbl_buyer_required_lc_limit.id
        ) AS lc_count
        FROM
          tbl_user
          LEFT JOIN tbl_network_requests ON tbl_user.id = tbl_network_requests.request_to
          LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_network_requests.request_from
          LEFT JOIN tbl_buyer_required_lc_limit ON tbl_buyer_required_lc_limit.createdBy = tbl_user.id
        WHERE
          tbl_user.type_id = '19' AND(
            tbl_buyer_required_lc_limit.createdAt >= '${from}' AND tbl_buyer_required_lc_limit.createdAt <= '${to}' ${extraQuery}
        )
        GROUP BY
          tbl_user_details.company_name
        ) AS combined_data
        WHERE company_name IS NOT NULL
        GROUP BY
          company_name
        ORDER BY
        (
            total_lc_value + total_inv_value
        )
        DESC
        LIMIT 10
        `
        console.log('Queryyyy ,',query)
        let response = await call({query}, 'makeQuery', 'get')  
        dbRes.message = response.message
      }
      
      resolve(dbRes)
    }catch(e){
      reject({
        success:false,
        message: {...e}
      })
    }
  })
}

exports.getApprovedApplicationByCountry = async (req,res) => {
  try{
    const result = await getApprovedApplicationByCountryFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getApprovedApplicationByCountryFunc = async ({onlyShowForUserId,userIds,from,to}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let extraQuery = ''
      if(onlyShowForUserId){
        extraQuery = ` AND (tbl_user.LeadAssignedTo = '${onlyShowForUserId}' OR tbl_user.SecondaryLeadAssignedTo = '${onlyShowForUserId}')`
      }
      if(userIds && userIds.length){
        extraQuery = ` AND (tbl_user.LeadAssignedTo IN ('${userIds.join("','")}') OR tbl_user.SecondaryLeadAssignedTo IN ('${userIds.join("','")}'))`
      }
      let dateRangeQuery = ` AND tbl_disbursement_scheduled.scheduledOn >= '${moment(from).format("YYYY-MM-DD")}' AND tbl_disbursement_scheduled.scheduledOn <= '${moment(to).format("YYYY-MM-DD")}'  `
      let disCountingQuery = `SELECT tbl_countries.name, COUNT(DISTINCT tbl_disbursement_scheduled.invRefNo) as totalCount FROM tbl_disbursement_scheduled
      LEFT JOIN tbl_invoice_discounting ON 
      tbl_invoice_discounting.reference_no = tbl_disbursement_scheduled.invRefNo
      LEFT JOIN tbl_buyers_detail ON 
      tbl_buyers_detail.id = tbl_invoice_discounting.buyer_id
      LEFT JOIN tbl_countries ON 
      tbl_countries.sortname = tbl_buyers_detail.buyerCountry
      LEFT JOIN tbl_buyer_required_lc_limit ON
      tbl_buyer_required_lc_limit.id = tbl_disbursement_scheduled.invRefNo
      LEFT JOIN tbl_user ON
      tbl_user.id = COALESCE(tbl_invoice_discounting.seller_id, tbl_buyer_required_lc_limit.createdBy)
      WHERE tbl_disbursement_scheduled.status = 1 AND tbl_buyers_detail.buyerCountry IS NOT NULL ${dateRangeQuery} ${extraQuery} GROUP BY tbl_buyers_detail.buyerCountry ORDER BY totalCount DESC LIMIT 10`;
      let disCountingRes = await call({ query: disCountingQuery }, 'makeQuery', 'get');
      resolve({
        success:true,
        message: disCountingRes.message
      })
    }catch(e){
      reject({
        success:false
      })
    }
  })
}

exports.getStageWiseApplicationStats = async (req,res) => {
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
      "activeUserApplicationSummaryAmount": {
        "Finance Limit": {},
        "Quote": {},
        "Termsheet/Contract": {},
        "Finance": {},
        "Agreement": {},
        "Approved": {}
      },
    }
    // Active User Application Stages
    let subQuery = ` BETWEEN '${reqBody.applicationStageFrom}' AND '${reqBody.applicationStageTo}' `
    
    // Applied but not received quote invoice
    query = `SELECT tbl_buyer_required_limit.id,tbl_buyer_required_limit.requiredLimit FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.buyers_credit IS NULL AND tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let suminvlimit = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.requiredLimit){
        suminvlimit += parseInt(element?.requiredLimit)
      }
    }
    response["activeUserApplicationSummary"]["Finance Limit"]["invoice"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Finance Limit"]["invoice"] = suminvlimit
    // Applied but not received quote lc
    query = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.financierQuotes IS NULL AND tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let lclimitsum = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.ocrFields?.["32B2"]){
        lclimitsum += parseInt(element?.ocrFields?.["32B2"])
      }
    }
    response["activeUserApplicationSummary"]["Finance Limit"]["lc"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Finance Limit"]["lc"] = lclimitsum


    // Applied and received quote but not selected financier invoice
    query = `SELECT tbl_buyer_required_limit.id,tbl_buyer_required_limit.requiredLimit FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.buyers_credit IS NOT NULL AND tbl_buyer_required_limit.selectedFinancier IS NULL AND
    tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let suminvQuote = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.requiredLimit){
        suminvQuote += parseInt(element?.requiredLimit)
      }
    }
    response["activeUserApplicationSummary"]["Quote"]["invoice"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Quote"]["invoice"] = suminvQuote

    // Applied and received quote but not selected financier lc
    query = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.financierQuotes IS NOT NULL AND tbl_buyer_required_lc_limit.selectedFinancier IS NULL AND
    tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let sumlcQuote = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.ocrFields?.["32B2"]){
        sumlcQuote += parseInt(element?.ocrFields?.["32B2"])
      }
    }
    response["activeUserApplicationSummary"]["Quote"]["lc"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Quote"]["lc"] = sumlcQuote


    // Applied and received termsheet but not applied for finance invoice
    query = `SELECT tbl_buyer_required_limit.id, tbl_buyer_required_limit.selectedQuote FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.termSheet IS NOT NULL AND tbl_buyer_required_limit.invRefNo IS NULL AND
    tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let suminvTermsheet = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.selectedQuote?.["financeLimit"]){
        suminvTermsheet += parseInt(element.selectedQuote?.["financeLimit"])
      }
    }
  
    response["activeUserApplicationSummary"]["Termsheet/Contract"]["invoice"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Termsheet/Contract"]["invoice"] = suminvTermsheet

    // Applied and received termsheet but not applied for finance lc
    query = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.reqLetterOfConfirmation IS NOT NULL AND tbl_buyer_required_lc_limit.invRefNo IS NULL AND
    tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let sumlcTS = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.ocrFields?.["32B2"]){
        sumlcTS += parseInt(element?.ocrFields?.["32B2"])
      }
    }
    response["activeUserApplicationSummary"]["Termsheet/Contract"]["lc"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Termsheet/Contract"]["lc"] = sumlcTS

    // Applied for finance invoice
    query = `SELECT tbl_buyer_required_limit.id,tbl_buyer_required_limit.selectedQuote FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE
    tbl_buyer_required_limit.invRefNo IS NOT NULL AND tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let suminvfinapply = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.selectedQuote?.["financeLimit"]){
        suminvfinapply += parseInt(element.selectedQuote?.["financeLimit"])
      }
    }
    response["activeUserApplicationSummary"]["Finance"]["invoice"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Finance"]["invoice"] = suminvfinapply

    // Applied for finance lc
    query = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.invRefNo IS NOT NULL AND tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    dbRes = await call({query},'makeQuery','get')
    let sumlcapplyFin = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.ocrFields?.["32B2"]){
        sumlcapplyFin += parseInt(element?.ocrFields?.["32B2"])
      }
    }
    response["activeUserApplicationSummary"]["Finance"]["lc"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Finance"]["lc"] = sumlcapplyFin

    // Agreement sent invoice
    query = `SELECT tbl_buyer_required_limit.id,tbl_buyer_required_limit.selectedQuote FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_buyers_detail ON
    tbl_buyer_required_limit.buyerId = tbl_buyers_detail.id
    WHERE tbl_buyer_required_limit.updatedAt ${subQuery} AND
    (tbl_buyer_required_limit.frameworkDoc IS NOT NULL OR tbl_buyer_required_limit.exhibitDoc IS NOT NULL OR tbl_buyer_required_limit.noaDoc IS NOT NULL) AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let suminvagreement = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.selectedQuote?.["financeLimit"]){
        suminvagreement += parseInt(element.selectedQuote?.["financeLimit"])
      }
    }
    response["activeUserApplicationSummary"]["Agreement"]["invoice"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Agreement"]["invoice"] = suminvagreement

    // Approved finance for invoice
    query = `SELECT tbl_buyer_required_limit.id,tbl_invoice_discounting.amount FROM tbl_buyer_required_limit 
    LEFT JOIN tbl_invoice_discounting ON
    tbl_buyer_required_limit.buyerId = tbl_invoice_discounting.buyer_id
    WHERE
    tbl_invoice_discounting.status = 3 AND tbl_buyer_required_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_limit.userId IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let suminvappfin = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.amount){
        suminvappfin += parseInt(element.amount)
      }
    }
    response["activeUserApplicationSummary"]["Approved"]["invoice"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Approved"]["invoice"] = suminvappfin

    // Approved finance for lc
    query = `SELECT tbl_buyer_required_lc_limit.id,tbl_buyer_required_lc_limit.ocrFields FROM tbl_buyer_required_lc_limit
    WHERE
    tbl_buyer_required_lc_limit.financeStatus = 1 AND tbl_buyer_required_lc_limit.updatedAt ${subQuery} AND
    tbl_buyer_required_lc_limit.createdBy IN (SELECT tbl_user.id FROM tbl_user WHERE last_login_at > '${lastActiveDateStr}')
    `
    dbRes = await call({query},'makeQuery','get')
    let sumlcappfin = 0
    for(let i = 0; i<= dbRes.message.length - 1; i++){
      const element = dbRes.message[i]
      if(element.ocrFields?.["32B2"]){
        sumlcappfin += parseInt(element?.ocrFields?.["32B2"])
      }
    }
    response["activeUserApplicationSummary"]["Approved"]["lc"] = dbRes.message.length
    response["activeUserApplicationSummaryAmount"]["Approved"]["lc"] = sumlcappfin

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