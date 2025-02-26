const CRMTasksLogs = require("../../src/database/Models/CRMTaskLogs")
const moment = require("moment");
const ExporterModelV2 = require("../../src/database/Models/ExporterModelV2");
exports.getCRMDashboardStats = async(req,res) => {
  try{
    const result = await getCRMDashboardStatsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
}

const getCRMDashboardStatsFunc = ({onlyShowForUserId,subadminIds}) => {
  return new Promise(async(resolve,reject) => {
    try{
      let pipelinedata = [
        {
          '$match': {
            $and:[
            {'STATUS': {
              '$in': [
                1, 3, 4
              ]
            }},
            onlyShowForUserId ? {'TASK_ASSIGNED_TO.id' : onlyShowForUserId} : {},
            subadminIds && subadminIds.length ? {'TASK_ASSIGNED_TO.id' : {$in: subadminIds}}: {}
            ]
            ,
            
          }
        }, {
          '$group': {
            '_id': '$STATUS', 
            'total_count': {
              '$sum': 1
            }, 
            'status': {
              '$first': '$STATUS'
            }
          }
        }
      ]
      let pipelinContacts = [
        {
          '$match': {
            $and : [
              {'EXTRA_DETAILS.Contact Number': {
                '$exists': true
              }},
              onlyShowForUserId ? {'TASK_ASSIGNED_TO.id' : onlyShowForUserId} : {},
              subadminIds && subadminIds.length ? {'TASK_ASSIGNED_TO.id' : {$in: subadminIds}}: {}
            ]
            
          }
        }, {
          '$count': 'total_contacts'
        }
      ]
      const response  = await ExporterModelV2.aggregate(pipelinedata)
      const contactsRes = await ExporterModelV2.aggregate(pipelinContacts)
      const finalRes = [...response,...contactsRes]
      resolve({
        success: true,
        message: finalRes
      })
    }catch(e){
      reject({
        success:false,
        message:[]
      })
    }
  })
}

exports.getTaskGraphs = async(req,res) => {
  try{
    const result = await getTaskGraphsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getTaskGraphsFunc = ({from,to,onlyShowForUserId,subadminIds}) => {
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
      const pipelinedata = [
        {
          '$match': {
            $and : [
              {'TASK_DATE': {
                '$exists': true
              }},
              onlyShowForUserId ? {'TASK_ASSIGNED_TO.id' : onlyShowForUserId} : {},
              subadminIds && subadminIds.length ? {'TASK_ASSIGNED_TO.id' : {$in: subadminIds}}: {},
              {
                'TASK_DATE' :{
                  $gte: new Date(from),
                  $lte: new Date(to)
                }
              }
            ]
            
          }
        }, {
          '$group': {
            '_id': {
              '$dateToString': {
                'format': dateFormat, 
                'date': '$TASK_DATE'
              }
            }, 
            'int_count': {
              '$sum': {
                '$cond': [
                  {
                    '$eq': [
                      '$STATUS', 1
                    ]
                  }, 1, 0
                ]
              }
            }, 
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
            'others_count': {
              '$sum': {
                '$cond': [
                  {
                    '$not': [
                      {
                        '$in': [
                          '$STATUS', [
                            1, 2
                          ]
                        ]
                      }
                    ]
                  }, 1, 0
                ]
              }
            }
          }
        }, {
          '$project': {
            '_id': 0, 
            'int_count': 1, 
            'not_interested': 1, 
            'others_count': {
              '$add': [
                '$others_count', {
                  '$sum': {
                    '$cond': [
                      {
                        '$in': [
                          '$STATUS', [
                            1, 2
                          ]
                        ]
                      }, 0, 1
                    ]
                  }
                }
              ]
            }, 
            'xLabel': '$_id'
          }
        },{
          '$sort' :{
            'xLabel': 1
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

exports.getLeadsGraphs = async(req,res) => {
  try{
    const result = await getLeadsGraphsFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getLeadsGraphsFunc = ({onlyShowForUserId,from,to,subadminIds}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let countForMonths =  moment(to).diff(from,'month')
      console.log('responseeeeee',countForMonths);
      if(countForMonths > 3){
        dateFormat = '%Y-%m-01'
      }else if(countForMonths <= 1){
        dateFormat = '%Y-%m-%d'
      }else{
        dateFormat = "W%V"
      }
      const pipelinedata = [
        {
          '$match': {
            $and : [
              {'TASK_DATE': {
                '$ne': null
              }},
              subadminIds && subadminIds.length ? {'TASK_ASSIGNED_TO.id' : {$in: subadminIds}}: {},
              onlyShowForUserId ? {'TASK_ASSIGNED_TO.id' : onlyShowForUserId} : {},
              {
                'TASK_DATE' :{
                  $gte: new Date(from),
                  $lte: new Date(to)
                }
              }
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
            '_id': {
              '$dateToString': {
                'format': dateFormat, 
                'date': '$TASK_DATE'
              }
            }, 
            'lead_count':{
              '$sum':1
            }
          }
        }, {
          '$project': {
            '_id': 0, 
            'lead_count':1,
            'xLabel': '$_id'
          }
        },{
          '$sort' :{
            'xLabel': 1
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


exports.getTasksBydate = async(req,res) => {
  try{
    const result = await getTasksBydateFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getTasksBydateFunc = ({eventdate,onlyShowForUserId,subadminIds}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let matchObj = {}
      if(onlyShowForUserId){
        matchObj = {
          'TASK_ASSIGNED_TO.id':onlyShowForUserId,
        }
      }
      if(subadminIds && subadminIds.length){
        matchObj = {
          'TASK_ASSIGNED_TO.id' : {$in: subadminIds}
        }
      }
      const pipelinedata = [
        {
          $match: matchObj
        },
        {
          '$match': {
            '$expr': {
              '$eq': [
                {
                  '$substr': [
                    '$EVENT_TIME', 0, 10
                  ]
                }, eventdate
              ]
            }
          }
        }, {
          '$match': {
            'LOG_TYPE': {
              '$in': [
                'Create New Task', 'Lead Created', 'Call back'
              ]
            }
          }
        }, {
          '$match': {
            'EVENT_TYPE': {
              '$regex': 'call|meet', 
              '$options': 'i'
            }
          }
        }, {
          '$project': {
            'EXPORTER_NAME': 1, 
            'EVENT_STATUS': 1, 
            'EVENT_TYPE': 1, 
            'EVENT_TIME': 1, 
            'LOG_TYPE': 1
          }
        }
      ]
      console.log(JSON.stringify(pipelinedata));
      const response = await CRMTasksLogs.aggregate(pipelinedata)
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


exports.getTasksCount = async(req,res) => {
  try{
    const result = await getTasksCountFunc(req.body)
    res.send(result)
  }catch(e){
    res.send(e)
  }
  
}

const getTasksCountFunc = ({fromDate,toDate,onlyShowForUserId,subadminIds}) => {
  return new Promise(async (resolve,reject) => {
    try{
      let matchObj = {}
      if(onlyShowForUserId){
        matchObj = {
          'TASK_ASSIGNED_TO.id':onlyShowForUserId
        }
      }
      if(subadminIds && subadminIds.length){
        matchObj = {
          'TASK_ASSIGNED_TO.id' : {$in: subadminIds}
        }
      }
      const pipelinedata = [
        {
          $match: matchObj
        },
        {
          '$match': {
            '$expr': {
              '$and': [
                {
                  '$lte': [
                    {
                      '$substr': [
                        '$TASK_DATE', 0, 10
                      ]
                    }, toDate
                  ]
                }, {
                  '$gte': [
                    {
                      '$substr': [
                        '$TASK_DATE', 0, 10
                      ]
                    }, fromDate
                  ]
                }
              ]
            }
          }
        }, {
          '$match': {
            'STATUS': {
              '$in': [
                0, 1, 2, 3
              ]
            }
          }
        }, {
          '$lookup': {
            'from': 'tbl_crm_tasks_logs', 
            'localField': 'EXPORTER_CODE', 
            'foreignField': 'EXPORTER_CODE', 
            'as': 'task_logs'
          }
        }, {
          '$project': {
            'EXPORTER_CODE': 1, 
            'EVENT_STATUS': {
              '$first': '$task_logs.EVENT_STATUS'
            }, 
            'LOG_TYPE': {
              '$first': '$task_logs.LOG_TYPE'
            },
            'STATUS':1
          }
        }
      ]
      const duepipeline = [
        {
          $match: matchObj
        },
        {
          '$match': {
            '$expr': {
              '$and': [
                {
                  '$lte': [
                    {
                      '$substr': [
                        '$TASK_DATE', 0, 10
                      ]
                    }, toDate
                  ]
                }, {
                  '$gte': [
                    {
                      '$substr': [
                        '$TASK_DATE', 0, 10
                      ]
                    }, fromDate
                  ]
                }
              ]
            }
          }
        }, {
          '$match': {
            '$expr': {
              '$lte': [
                {
                  '$substr': [
                    '$TASK_DATE', 0, 10
                  ]
                }, moment().subtract(1,'days').format('YYYY-MM-DD')
              ]
            }
          }
        }, {
          '$match' :{
            'STATUS' : {'$in' : [0,1]}
          }
        },
        
        {
          '$lookup': {
            'from': 'tbl_crm_tasks_logs', 
            'localField': 'EXPORTER_CODE', 
            'foreignField': 'EXPORTER_CODE', 
            'as': 'task_logs'
          }
        }, {
          '$project': {
            'EXPORTER_CODE': 1, 
            'EVENT_STATUS': {
              '$first': '$task_logs.EVENT_STATUS'
            }, 
            'LOG_TYPE': {
              '$first': '$task_logs.LOG_TYPE'
            }, 
            'STATUS': 1
          }
        }
      ]
      const response = await ExporterModelV2.aggregate(pipelinedata)
      let hot_count = 0
      let not_int_count = 0
      let cold_count = 0
      let lost_count = 0
      let warm_count = 0
      let pending_count = 0
      let total_count= 0
      for(let i =0; i<=response.length - 1; i++){
        const element = response[i]
        total_count += 1
        if(element.STATUS == 2){
          not_int_count += 1
        }else if(element.STATUS == 3){
          lost_count +=1
        }else if(element.EVENT_STATUS?.includes("Cold")){
          cold_count += 1
        }else if(element.EVENT_STATUS?.includes("Hot")){
          hot_count += 1
        }else if(element.EVENT_STATUS?.includes("Warm")){
          warm_count +=1
        }else{
          pending_count +=1
        }
      }
      const dureresponse = await ExporterModelV2.aggregate(duepipeline)
      let hot_count_due = 0
      let cold_count_due = 0
      let warm_count_due = 0
      let pending_count_due = 0
      let total_count_due= 0
      for(let i =0; i<=dureresponse.length - 1; i++){
        const element = dureresponse[i]
        total_count_due += 1
        if(element.EVENT_STATUS?.includes("Cold")){
          cold_count_due += 1
        }else if(element.EVENT_STATUS?.includes("Hot")){
          hot_count_due += 1
        }else if(element.EVENT_STATUS?.includes("Warm")){
          warm_count_due +=1
        }else{
          pending_count_due +=1
        }
      }
      resolve({
        success:true,
        message:{
          taskCreated : [{
            hot_count,
            warm_count,
            cold_count,
            not_int_count,
            lost_count,
            pending_count,
            total_count
          }],
          taskDue : [{
            hot_count_due,
            warm_count_due,
            cold_count_due,
            pending_count_due,
            total_count_due
          }]
        }
      })
    }catch(e){
      console.log('error in api',e);
      reject({
        success:false,
        message:{
          taskCreated : [{
            hot_count:0,
            warm_count:0,
            cold_count:0,
            not_int_count:0,
            lost_count:0,
            pending_count:0,
            total_count:0
          }],
          taskDue : [{
            hot_count_due:0,
            warm_count_due:0,
            cold_count_due:0,
            not_int_count_due:0,
            lost_count_due:0,
            pending_count_due:0,
            total_count_due:0
          }]
        }
      })
    }
  })
}
