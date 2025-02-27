const { call } = require('../../utils/codeHelper');
const Request = require("request");
var mysql = require('mysql2');
const fs = require('fs');
const xlsx = require('xlsx');
const { getCurrentTimeStamp, formatSqlQuery, apiCall } = require('../../iris_server/utils');
const { nullCheck } = require('../../database/utils/utilFuncs');
const { dbPool } = require('../../src/database/mysql');
const config = require('../../config');
const { bccEmails, mailFooter, aiServerBackendUrl, platformURL, encryptData } = require('../../urlCostants');
const { sendMail } = require('../../utils/mailer');
const { adjustQuantityTrail } = require('../commodityControllers/setCommodityList');
const { setContractLogs } = require('../../utils/setContractLog')
exports.updateSalesPurchaseQuotation = async (req, res, next) => {
  try {
    let ipArray = req.ip.split(":")
    let finalIp = ipArray[ipArray.length - 1]
    let { data, sellerId, docId, buyerId, status, appId, docType, transaction_timeline,cameForEditingChangelogs,initialDataForContainer ,changedBy,contractId ,userId} = req.body
    console.log("updateSalesPurchaseQuotation", sellerId, buyerId, status, appId, docType)
    await dbPool.query(formatSqlQuery(`UPDATE tbl_sales_purchase_quotations SET sellerId = ?, buyerId = ?, details =? , status = ? ,transaction_timeline=?,docType=?,docId=?
      WHERE id = ? `, [sellerId, buyerId, JSON.stringify(data), status, JSON.stringify(transaction_timeline), docType, docId, appId]))
    console.log("InupdateSalesPurchaseQuotation done");
      if(contractId){
        setContractLogs({
          contractNo: contractId,
          status: "Updated",
          message: `${docType} Updated`,
          created_by: userId,
          
          network_json: { ip: finalIp }
        })
          .then((response) => {
            console.log("response in setContractLogs =>", response)
          })
          .catch((error) => {
            console.log("error in setContractLogs =>", error)
          })
      }
      
    if (cameForEditingChangelogs) {
      // Check if docId exists in tbl_change_logs_container
      const [existingLog] = await dbPool.query(formatSqlQuery(
        `SELECT newData FROM tbl_change_logs_container WHERE docId = ?`, 
        [docId]
      ));
      console.log("existingLog",existingLog)
      let oldData, newData;

      if (existingLog.length > 0) {
        oldData = existingLog[0].newData; // Set oldData as previously stored newData
        newData = data; // Set newData as the updated data
        await dbPool.query(formatSqlQuery(
          `DELETE FROM tbl_change_logs_container WHERE docId = ?`,
          [docId]
        ));
      } else {
        oldData = initialDataForContainer; // If no record exists, use initial data
        newData = data; // Set newData as the updated data
      }

      // Insert or update change logs
      await dbPool.query(formatSqlQuery(
        `INSERT INTO tbl_change_logs_container (docId, oldData, newData, changedBy) 
         VALUES (?, ?, ?, ?)`,
        [docId, JSON.stringify(oldData), JSON.stringify(newData), changedBy]
      ));
      
      
    }
    res.send({ success: true })

  } catch (error) {
    console.log("errorInupdateSalesPurchaseQuotation", error);
    res.send({ success: false })
  }
}
function generateSalesQuotationNumber() {
  const now = new Date();

  const year = now.getFullYear().toString().slice(-2);
  const month = (now.getMonth() + 1).toString().padStart(2, '0');
  const day = now.getDate().toString().padStart(2, '0');
  const hours = now.getHours().toString().padStart(2, '0');
  const minutes = now.getMinutes().toString().padStart(2, '0');
  const seconds = now.getSeconds().toString().padStart(2, '0');
  const milliseconds = now.getMilliseconds().toString().padStart(3, '0'); // Add milliseconds
  const randomSuffix = Math.floor(Math.random() * 1000).toString().padStart(3, '0'); // Random number between 0-999

  // ${year}
  const docNumber = `SE${month}${day}${hours}${minutes}${seconds}${randomSuffix}`;

  return docNumber;
}

exports.createSalesPurchaseQuotation = async (req, res, next) => {
  try {
    
    let { userId, data, docId, sellerId, buyerId, appLink, status, docType, transaction_timeline ,contractId } = req.body
    // console.log(req.body ,"req body=>>>>>>>>>.")
    let ipArray = req.ip.split(":")
    let finalIp = ipArray[ipArray.length - 1]
   
    
    //   await dbPool.query(formatSqlQuery(`INSERT INTO tbl_sales_purchase_quotations (docId,docType, sellerId, buyerId, details, status,transaction_timeline, createdBy) VALUES (?,?,?,?,?,?,?,?)`,
    //   [docId, docType, sellerId, buyerId, JSON.stringify(data), status, JSON.stringify(transaction_timeline), userId]
    // ))
    if (docType === "Request Quotation") {
      const allSellers = JSON.parse(sellerId);
      console.log("see all sellers:",allSellers)
      await Promise.all(
        allSellers.map(async (seller) => {
          const uniqDocNumber = generateSalesQuotationNumber()
          const dataForSE = data?.[seller]
                              ? {
                                  ...data, 
                                  ...data[seller],
                                }
                              : data;
          dataForSE.SENumber = uniqDocNumber
          dataForSE.SEDate = new Date().toISOString().split('T')[0] 
          const dataForTransaction_timeline = { 
            ...transaction_timeline, 
            "Sales Enquiry": new Date().toLocaleString('en-US', { 
              year: 'numeric', 
              month: 'numeric', 
              day: 'numeric', 
              hour: 'numeric', 
              minute: 'numeric', 
              second: 'numeric', 
              hour12: true 
            })
        };
        
          await dbPool.query(
            formatSqlQuery(
              `INSERT INTO tbl_sales_purchase_quotations 
              (docId, docType, sellerId, buyerId, details, status, transaction_timeline, createdBy) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
              [uniqDocNumber, "Sales Enquiry", seller, buyerId, JSON.stringify(dataForSE), status, JSON.stringify(dataForTransaction_timeline), userId]
            )
          );
        })
      );
    }
    if(docType === "Commercial Invoice"){
      await dbPool.query(formatSqlQuery(`INSERT INTO tbl_sales_purchase_quotations (docId,docType, sellerId, buyerId, details, status,transaction_timeline, createdBy,contractId) VALUES (?,?,?,?,?,?,?,?,?)`,
      [docId, docType, sellerId, buyerId, JSON.stringify(data), status, JSON.stringify(transaction_timeline), userId,contractId],
       
      
    ))

    
    }
    if(docType === "Proforma Invoice"){
      await dbPool.query(formatSqlQuery(`INSERT INTO tbl_sales_purchase_quotations (docId,docType, sellerId, buyerId, details, status,transaction_timeline, createdBy,contractId) VALUES (?,?,?,?,?,?,?,?,?)`,
      [docId, docType, sellerId, buyerId, JSON.stringify(data), status, JSON.stringify(transaction_timeline), userId,contractId]
    ))
    }
    if(docType === "Order Confirmation" && contractId){
      await dbPool.query(formatSqlQuery(`INSERT INTO tbl_sales_purchase_quotations (docId,docType, sellerId, buyerId, details, status,transaction_timeline, createdBy,contractId) VALUES (?,?,?,?,?,?,?,?,?)`,
      [docId, docType, sellerId, buyerId, JSON.stringify(data), status, JSON.stringify(transaction_timeline), userId,contractId]
    ))
    }else{
      await dbPool.query(formatSqlQuery(`INSERT INTO tbl_sales_purchase_quotations (docId,docType, sellerId, buyerId, details, status,transaction_timeline, createdBy,contractId) VALUES (?,?,?,?,?,?,?,?,?)`,
      [docId, docType, sellerId, buyerId, JSON.stringify(data), status, JSON.stringify(transaction_timeline), userId,contractId]
    ))
  }

  if(contractId){
    setContractLogs({
      contractNo: contractId,
      status: "Created",
      message: `${docType} Created`,
      created_by: userId,
      
      network_json: { ip: finalIp }
    })
      .then((response) => {
        console.log("response in setContractLogs =>", response)
      })
      .catch((error) => {
        console.log("error in setContractLogs =>", error)
      })
  }


    if(docType === "Invoice"){
      const itemDescCount = Object.keys(data).filter(key => /^itemDesc\d+$/.test(key)).length;
      if(itemDescCount){
        for(let i = 0; i<itemDescCount; i++){
          let salesObj = {
            commodityPrettyName:data[`itemDesc${i}`],
            date:data.invoiceDate || data.taxInvoiceDate,
            quantity:-parseInt(data[`itemQuantity${i}`]),
            rate:data[`itemUnitPrice${i}`],
            usedFor:docId,
            userId:userId
        }
        let QuantityAdjust = await adjustQuantityTrail({body:salesObj})
        console.log("QuantityAdjust",QuantityAdjust)
        }
      }
      
    }
    if(docType === "Purchase Tax Invoice"){
      const itemDescCount = Object.keys(data).filter(key => /^itemDesc\d+$/.test(key)).length;
      if(itemDescCount){
        for(let i = 0; i<itemDescCount; i++){
          let salesObj = {
            commodityPrettyName:data[`itemDesc${i}`],
            date:data.invoiceDate || data.taxInvoiceDate,
            quantity:parseInt(data[`itemQuantity${i}`]),
            rate:data[`itemUnitPrice${i}`],
            usedFor:docId,
            userId:userId
        }
        let QuantityAdjust = await adjustQuantityTrail({body:salesObj})
        console.log("QuantityAdjust",QuantityAdjust)
        }
        
      }
    }
    let sellerDetails = await call({ query: formatSqlQuery(`SELECT email_id, company_name FROM tbl_user_details WHERE tbl_user_id = ?`, [sellerId]) }, 'makeQuery', 'get');
    let buyerDetails = await call({ query: formatSqlQuery(`SELECT email_id, company_name FROM tbl_user_details WHERE tbl_user_id = ?`, [buyerId]) }, 'makeQuery', 'get');
    //   <p style="padding-left: 3rem;font-size: 18px;word-spacing: 1.8px;
    // margin-top: 0px;">
    // New Sales Quotation has been created with the ${buyerDetails.message?.[0]?.company_name}, to view the quotation details click on the below link - <br><br>
    // ${appLink}
    // </p>


    // update RFQ based on SQ Created

      
    if (docType === "Sales Quotation" && data.RFQNumber && status === 0) {
      console.log("Inside Sales Quotation block");
      try {
        // Query the record with the matching docId using the "details" column
        const selectQuery = formatSqlQuery(
          "SELECT details FROM tbl_sales_purchase_quotations WHERE docId = ?",
          [data.RFQNumber]
        );
        const [rows] = await dbPool.query(selectQuery);
        console.log("Query result:", rows);
        
        if (rows && rows.length > 0) {
          const record = rows[0];
          // Use the "details" column (not "data")
          let jsonData = typeof record.details === "object" ? record.details : JSON.parse(record.details);
          
          // Build the QuoteDetails array for all items
          let quoteDetails = [];
          let i = 0;
          while (data[`itemQuantity${i}`] !== undefined) {
            const quotedQuantity =
              data[`itemQuantity${i}`] + (data[` itemQuantityUnits${i}`] || "");
            // Build the QuoteDetails object for this item.
            quoteDetails.push({
              "SQ No": docId,
              "Item Name": data[`itemDesc${i}`],
              "Quoted Quantity": quotedQuantity,
              "Price Per Unit": data[`itemUnitPrice${i}`],
              "Tax %": data[`itemTax%${i}`],
              "Total Amount": data[`itemTotalAmount${i}`]
            });
            i++;
          }
          console.log("QuoteDetails:", quoteDetails);
          
          // Update jsonData with QuoteDetails using sellerId, if provided.
          if (data.sellerId) {
            jsonData[data.sellerId] = jsonData[data.sellerId] || {};
            jsonData[data.sellerId].QuoteDetails = quoteDetails;
          } else {
            jsonData.QuoteDetails = quoteDetails;
          }
          
          const updateQuery = formatSqlQuery(
            "UPDATE tbl_sales_purchase_quotations SET details = ? WHERE docId = ?",
            [JSON.stringify(jsonData), data.RFQNumber]
          );
          await dbPool.query(updateQuery);
          console.log("Record updated successfully");
        }
      } catch (error) {
        console.error("Error updating sales quotation:", error);
      }
    }
    
     
    
    if (status === 0) {
      console.log("send mail to seller")
      // send mail to seller
      let mailOptions = {
        from: config.mail.user,
        to: [sellerDetails.message?.[0]?.email_id],
        bcc: bccEmails,
        subject: `New ${docType} Created`,
        html: `
        <body style="margin: 0px;">
        <div style="max-width: 795px; width: 100%;">
          <div style="width: 100%;background-image: url('https://tradereboot.s3.ap-south-1.amazonaws.com/Emailers/selectquoteMailer.png');background-repeat: no-repeat;
          background-size: contain; height: 560px;
        width: 100%;">
            <div style="max-width: 480px;">
              <p style="padding-top: 8.5rem;padding-left: 3rem;font-size: 20px;word-spacing: 2px;">
                <b>Dear ${sellerDetails.message?.[0]?.company_name}</b>
              </p>
      
              <p style="padding-left: 3rem;font-size: 18px;word-spacing: 1.8px;
              margin-top: 0px;">
                New ${docType} has been created with the ${buyerDetails.message?.[0]?.company_name}, and shared with you .
              <a href="${appLink}" target="_blank">${appLink}</a>
              </p>
      
              <p style="padding-left: 3rem;font-size: 20px;line-height: 1.6rem;">
                Incase of further assisstance drop us a message here
                <!-- <p style="padding-left:3rem;font-size: 18px;word-spacing: 2px;"> -->
                <label style="text-align: center; 
                  font-size: 20px; 
                  background: EBFE1;
                  padding-left: 1.1rem;
                  padding-right: 1.1rem;
                  border-radius: 1000px;
                  /* padding-top: 0.2rem;
                  padding-bottom: 0.2rem; */
                  width: auto;">
                  +91 74001 54262
                </label>
                <!-- </p> -->
                </br>
                <span style="font-size: 20px; ">or write to us at </span>
      
      
                <!-- <p style="padding-left:3rem;font-size: 18px;word-spacing: 2px;"> -->
                <label style="text-align: center; 
                  font-size: 20px; 
                  padding-left: 1.1rem;
                  padding-right: 1.1rem;
                  /* padding-top: 0.2rem;
                  padding-bottom: 0.2rem; */
                  border-radius: 20px;
                  width: auto;
                  border: 3px solid #1EA3AF;
                  ">
                  <a href="mailto:info@tradereboot.com" target="_blank" style="text-decoration: none;color:black">
                  info@tradereboot.com
                  </a>
                  
                </label>
                <!-- </p> -->
      
              </p>
              <p style="font-size: 10px;padding-left: 3rem;">*T&C applied</p>
      
            </div>
      
      
          </div>
        </div>
      </body>
        `
      }
      sendMail(mailOptions)

      // send mail to buyer
      mailOptions = {
        from: config.mail.user,
        to: [buyerDetails.message?.[0]?.email_id],
        bcc: bccEmails,
        subject: `New ${docType} Created`,
        html: `
        <body style="margin: 0px;">
        <div style="max-width: 795px; width: 100%;">
          <div style="width: 100%;background-image: url('https://tradereboot.s3.ap-south-1.amazonaws.com/Emailers/selectquoteMailer.png');background-repeat: no-repeat;
          background-size: contain; height: 560px;
        width: 100%;">
            <div style="max-width: 480px;">
              <p style="padding-top: 8.5rem;padding-left: 3rem;font-size: 20px;word-spacing: 2px;">
                <b>Dear ${buyerDetails.message?.[0]?.company_name}</b>
              </p>
      
              <p style="padding-left: 3rem;font-size: 18px;word-spacing: 1.8px;
              margin-top: 0px;">
                New ${docType} has been created with the ${sellerDetails.message?.[0]?.company_name}, and shared with you.
                <a href="${appLink}" target="_blank">${appLink}</a>
              </p>
      
              <p style="padding-left: 3rem;font-size: 20px;line-height: 1.6rem;">
                Incase of further assisstance drop us a message here
                <!-- <p style="padding-left:3rem;font-size: 18px;word-spacing: 2px;"> -->
                <label style="text-align: center; 
                  font-size: 20px; 
                  background: EBFE1;
                  padding-left: 1.1rem;
                  padding-right: 1.1rem;
                  border-radius: 1000px;
                  /* padding-top: 0.2rem;
                  padding-bottom: 0.2rem; */
                  width: auto;">
                  +91 74001 54262
                </label>
                <!-- </p> -->
                </br>
                <span style="font-size: 20px; ">or write to us at </span>
      
      
                <!-- <p style="padding-left:3rem;font-size: 18px;word-spacing: 2px;"> -->
                <label style="text-align: center; 
                  font-size: 20px; 
                  padding-left: 1.1rem;
                  padding-right: 1.1rem;
                  /* padding-top: 0.2rem;
                  padding-bottom: 0.2rem; */
                  border-radius: 20px;
                  width: auto;
                  border: 3px solid #1EA3AF;
                  ">
                  <a href="mailto:info@tradereboot.com" target="_blank" style="text-decoration: none;color:black">
                  info@tradereboot.com
                  </a>
                  
                </label>
                <!-- </p> -->
      
              </p>
              <p style="font-size: 10px;padding-left: 3rem;">*T&C applied</p>
      
            </div>
      
      
          </div>
        </div>
      </body>
        `
      }
      sendMail(mailOptions)
    }

    res.send({
      success: true
    })
  } catch (error) {
    console.log("errorincreateSalesPurchaseQuotation", error)
    res.send({
      success: false
    })
  }
}

exports.getSalesPurchaseQuotation = async (req, res, next) => {
  try {
    
    let { userId, currentPage, resultPerPage, search,byDocName } = req.body
    let searchQuery = ''
    // if (search) {
    //   searchQuery += ` AND (
    //   tbl_sales_purchase_quotations.docId LIKE '%${search}%'
    // ) `
    // }
    if(byDocName && Array.isArray(byDocName) && byDocName.length > 0){
      const DocNames = byDocName.map(doc => `'${doc}'`).join(", ");
      searchQuery += ` AND (
        tbl_sales_purchase_quotations.docType IN (${DocNames})
      ) `;
    }
    if (search && Array.isArray(search) && search.length > 0) {
      console.log("came inside of search ids")
      // Build the search query using 'IN' operator for matching multiple docIds
      const searchIds = search.map(id => `'${id}'`).join(", ");
      
      searchQuery += ` AND (
        tbl_sales_purchase_quotations.docId IN (${searchIds})
      ) `;
    }
    // LIMIT ${resultPerPage} OFFSET ${(currentPage - 1) * resultPerPage
    let query = `SELECT * 
    FROM tbl_sales_purchase_quotations 
    WHERE (sellerId = ${userId} OR buyerId = ${userId}) 
      AND NOT (docType = 'Sales Enquiry' AND buyerId = ${userId})
    ${searchQuery}
    ORDER BY tbl_sales_purchase_quotations.id DESC
    `
    

    let dbRes = await call({ query }, 'makeQuery', 'get');
    console.log("query for search is",query,dbRes.message.length)

  let salesQuery = `
  SELECT * 
  FROM tbl_sales_purchase_quotations 
  WHERE (sellerId = ${userId} OR buyerId = ${userId}) 
  AND (docType IN ("Sales Enquiry", "Sales Quotation", "Request Quotation", "Order Confirmation", "Invoice","Delivery Challan","Credit Note"))
    AND NOT (docType = "Sales Enquiry" AND buyerId = ${userId})
    AND NOT (docType = "Request Quotation" AND (
    sellerID = '${userId}' OR 
    JSON_CONTAINS(sellerID, '"${userId}"')
  ))
  ORDER BY tbl_sales_purchase_quotations.id DESC
`;

  let purchaseQuery = `
     SELECT * FROM tbl_sales_purchase_quotations 
    WHERE (sellerId = ${userId} OR buyerId = ${userId}) 
        AND (docType IN ("Purchase Order","Purchase Tax Invoice","Inward Document","Goods Received Note","Debit Note"))

    ORDER BY tbl_sales_purchase_quotations.id DESC`
  


    let countData = {}
    let dbResp2 = null
    let countQuery

    countQuery = `SELECT docType, COUNT(*) as count 
                  FROM tbl_sales_purchase_quotations 
                  WHERE (sellerId = ${userId} OR buyerId = ${userId}) 
                  GROUP BY docType;
`
    dbResp2 = await call({ query: countQuery }, 'makeQuery', 'get');
    console.log(dbResp2)
    countData = dbResp2.message

    
    // Execute sales and purchase queries
    let dbResSales = {}
    let dbResPurchase ={}
    if(!search && !byDocName){
      dbResSales = await call({ query: salesQuery }, 'makeQuery', 'get');
      dbResPurchase = await call({ query: purchaseQuery }, 'makeQuery', 'get');
    }
      res.send({
      success: true,
      data: {
        data: dbRes.message,
        salesDocs: dbResSales.message,
        purchaseDocs: dbResPurchase.message,
        countData:countData
      }
    })
  } catch (error) {
    console.log("errorIngetSalesPurchaseQuotation", error);
    res.send({
      success: false
    })
  }
}



exports.getTags = async (req, res, next) => {
  try {
    let { type } = req.body
    let query = formatSqlQuery(`SELECT * FROM tbl_tags WHERE type = ? `, [type])
    let dbRes = await call({ query }, 'makeQuery', 'get');
    res.send({
      success: true,
      data: dbRes.message
    })
  } catch (error) {
    console.log("errorIngetTags", error);
    res.send({
      success: false
    })
  }
}
exports.getChangeLogsForContainer=async (req, res, next) => {
  try{
    let { docId } = req.body;

// Ensure docId is an array, and format it correctly
if (!Array.isArray(docId) || docId.length === 0) {
    return res.json([]); // Return an empty response if docId is empty
}

let query = formatSqlQuery(
    `SELECT * FROM tbl_change_logs_container WHERE docId IN (?)`,
    [docId]
);

    let dbRes = await call({ query }, 'makeQuery', 'get');
    res.send({
      success: true,
      data: dbRes.message
    })
  }catch(e){
    console.log("errorIncreateNewTag", error);
    res.send({
      success: false
    })
  }
}
exports.createNewTag = async (req, res, next) => {
  try {
    let { type, name } = req.body
    await dbPool.query(formatSqlQuery(`DELETE FROM tbl_tags WHERE type = ? AND name = ? `, [type, name]))
    await dbPool.query(formatSqlQuery(`INSERT INTO tbl_tags (type, name) VALUES (?,?)`, [type, name]))
    res.send({
      success: true
    })
  } catch (error) {
    console.log("errorIncreateNewTag", error);
    res.send({
      success: false
    })
  }
}

exports.newOCR = async(req,res,next)=>{
  const{fileObj} = req.files
  console.log(fileObj ,"this is fileOBj---")
  
	try {
   
  
      let options = {
        method: "POST",
        url: `${aiServerBackendUrl}/get_json_details_from_gpt`,
        headers: {
          "Content-Type": "multipart/form-data"
        },
        formData: {
          "doc": {
            value: fileObj.data,
            options: {
              filename: fileObj.name,
              contentType: "application/pdf"
            }
          },
          "defaultResp": JSON.stringify({ sales: null, currency: null }),
          "prompt": `Analyze below document and give me details like {PO Number: (integer), PO Amount: (float)}, in a json format. 
          
          docTxtData`
        }
      };
      let aiApiResp = await apiCall(null, null, null, null, options)
      aiApiResp = JSON.parse(aiApiResp)
			console.log(aiApiResp,"this is resp")
			res.send({data:aiApiResp})

  } catch (error) {
    console.log("errorInanalyzeGstFileObj", error);
  }
}

exports.sendEmailWithPDF = async (req, res) => {
  console.log("Request came")
  try {
    const { email, message, document, subject } = req.body;

    const mailOptions = {
      from: config.mail.user,
      to: [email],
      subject: subject,
      html: message,

    };
    let docsMetaData = []
    let query = `SELECT * FROM tbl_document_details WHERE id = '${element}' `
    let dbRes = await call({ query }, 'makeQuery', 'get');
    if (dbRes.message.length) {
      docsMetaData.push({ filename: `${dbRes.message[0]["doc_name"]} - ${dbRes.message[0]["file_name"]}`, path: path.resolve(__dirname, `../../docs/${dbRes.message[0]["file_hash"]}`) })
    }
    if (docsMetaData?.length) {
      mailOptions["attachments"] = docsMetaData
    }


    sendMail(mailOptions);
    console.log(mailOptions)
    res.send({ success: true, message: "Email sent successfully" });
  }
  catch (e) {
    console.log(e)
  }
}

// Send SMS API
// app.post('/sendSMS', (req, res) => {
//   const { phoneNumber, message } = req.body;

//   twilioClient.messages.create({
//     body: message,
//     from: 'your-twilio-phone-number',
//     to: phoneNumber
//   })
//   .then(message => {
//     console.log('Message sent:', message.sid);
//     res.send('Message sent successfully');
//   })
//   .catch(error => {
//     console.log('Error sending message:', error);
//     res.status(500).send('Error sending message');
//   });
// });



// exports.getSalesPurchaseQuotation = async (req, res, next) => {
//   try {
//     let { userId, currentPage, resultPerPage, search } = req.body
//     let searchQuery = ''
//     if (search) {
//       searchQuery += `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) ${searchQuery}
//  AND (
//       tbl_sales_purchase_quotations.details[""] LIKE '%${search}%'
//     ) `
//     }


//     res.send({
//       success: true,
//       data: { data: dbRes.message, count: countDbRes.message.length, countData }
//     })
//   } catch (error) {
//     console.log("errorIngetSalesPurchaseQuotation", error);
//     res.send({
//       success: false
//     })
//   }
// }



// let countQuery = `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) ${searchQuery} `
// let countDbRes = await call({ query: countQuery }, 'makeQuery', 'get');

// let countData = {}
// let dbResp2 = null

// countQuery = `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) AND status = 0 `
// dbResp2 = await call({ query: countQuery }, 'makeQuery', 'get');
// countData["Pending"] = dbResp2.message.length

// countQuery = `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) AND status = 1 `
// dbResp2 = await call({ query: countQuery }, 'makeQuery', 'get');
// countData["Draft"] = dbResp2.message.length

// countQuery = `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) AND status = 2 `
// dbResp2 = await call({ query: countQuery }, 'makeQuery', 'get');
// countData["Lost"] = dbResp2.message.length

// countQuery = `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) AND status = 3 `
// dbResp2 = await call({ query: countQuery }, 'makeQuery', 'get');
// countData["Won"] = dbResp2.message.length

// countQuery = `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) AND status = 4 `
// dbResp2 = await call({ query: countQuery }, 'makeQuery', 'get');
// countData["Cancelled"] = dbResp2.message.length

// countQuery = `SELECT * FROM tbl_sales_purchase_quotations WHERE (sellerId = ${userId} OR buyerId = ${userId}) AND status = 5 `
// dbResp2 = await call({ query: countQuery }, 'makeQuery', 'get');
// countData["Cancelled"] = dbResp2.message.length
// count: countDbRes.message.length,

exports.bulkInventory = async (req, res, next) => {
  try {
    
    let { userId, payloadData, docId, traderType } = req.body
   
    
       console.log(req.body,"this is req body in bulk inventory????????")
       if(traderType === "Buyer"){

       
          let salesObj = {
            commodityPrettyName:payloadData[`itemDesc`],
            date:payloadData.bulkBreakDate ,
            quantity:parseInt(payloadData[`itemQuantity`]),
            rate:payloadData[`itemUnitPrice`],
            usedFor:docId,
            userId:userId
        }
        let QuantityAdjust = await adjustQuantityTrail({body:salesObj})
        console.log("QuantityAdjust",QuantityAdjust)
        console.log("Quantity updated-->>>>>>>>>>")
      }else if (traderType === "Seller"){
        let salesObj = {
          commodityPrettyName:payloadData[`itemDesc`],
          date:payloadData.bulkBreakDate ,
          quantity:-parseInt(payloadData[`itemQuantity`]),
          rate:payloadData[`itemUnitPrice`],
          usedFor:docId,
          userId:userId
      }
      let QuantityAdjust = await adjustQuantityTrail({body:salesObj})
      console.log("QuantityAdjust",QuantityAdjust)
      console.log("Quantity updated-->>>>>>>>>>")
      }
        
    res.send({
      success: true
    })
  } catch (error) {
    console.log("errorincreateSalesPurchaseQuotation", error)
    res.send({
      success: false
    })
  }
}


exports.commodityBulkBreakLog = async (req, res, next) => {
  try {
    
    let { userId, contractNo } = req.body
   
    let query = `SELECT * FROM tbl_commodity_details, JSON_TABLE( quantityTrail, '$[*]' COLUMNS ( date BIGINT PATH '$.date', rate VARCHAR(10) PATH '$.rate', usedFor VARCHAR(50) PATH '$.usedFor', closing_quantity INT PATH '$.closing_quantity', opening_quantity INT PATH '$.opening_quantity' ) ) AS data_table WHERE data_table.usedFor = '${contractNo}' ORDER BY data_table.date DESC`
    
    let dbRes = await call({ query }, 'makeQuery', 'get');
    res.send({
      success: true,
      message:dbRes.message
    })
  } catch (error) {
    console.log("errorincreateSalesPurchaseQuotation", error)
    res.send({
      success: false
    })
  }
}


exports.getQuoatationById = async (req, res, next) => {
  try {
    
    let {contractId ,docType} = req.body
    let {docId} = req.query
   
    let query = `SELECT * FROM tbl_sales_purchase_quotations WHERE tbl_sales_purchase_quotations.contractId ='${contractId}' AND docType = '${docType}'`
    if(docId){
      query = `SELECT * FROM tbl_sales_purchase_quotations WHERE tbl_sales_purchase_quotations.docId = '${docId}'`
    }
    let dbRes = await call({ query }, 'makeQuery', 'get');
    res.send({
      success: true,
      data:dbRes.message,
    })
  } catch (error) {
    console.log("errorincreateSalesPurchaseQuotation", error)
    res.send({
      success: false
    })
  }
}

exports.getContainerDocsForContractId = async (req, res, next) => {
  try {
    let { contractId, totalbulkdoc } = req.body;
    let query=''
    let shipmentqry =''
    if(contractId){
   query += `SELECT * FROM tbl_sales_purchase_quotations WHERE tbl_sales_purchase_quotations.contractId ='${contractId}'`;
    }
    let countQuery=''
    if (totalbulkdoc) {
      query = `SELECT * FROM tbl_sales_purchase_quotations WHERE tbl_sales_purchase_quotations.contractId IS NOT NULL AND docType IN ('Purchase Order', 'Proforma Invoice', 'Commercial Invoice')`;
      countQuery = `SELECT COUNT(*) AS total FROM tbl_sales_purchase_quotations WHERE tbl_sales_purchase_quotations.contractId IS NOT NULL AND docType IN ('Purchase Order', 'Proforma Invoice', 'Commercial Invoice')`
   
        shipmentqry=` SELECT COUNT(*) AS total FROM tbl_shipment_booking_application WHERE tbl_shipment_booking_application.bulkcreate IS NOT NULL`
      
    }

    let dbRes = await call({ query }, 'makeQuery', 'get');
    let dbCount = await call({ query: countQuery }, 'makeQuery', 'get');
    let dbShip = await call({query:shipmentqry},'makeQuery','get')
    res.send({
      success: true,
      data: dbRes.message,
      count:dbCount.message,
      shipmentCount:dbShip.message
    });
  } catch (error) {
    console.log("errorincreateSalesPurchaseQuotation", error);
    res.status(500).send({ success: false, message: "Internal server error" });
  }
};

exports.getContractStats = async (req, res, next) => {
  try {
    const { totalbulkdoc } = req.body;

    if (!totalbulkdoc) {
      return res.status(400).send({ success: false, message: "totalbulkdoc is required" });
    }

    const query = `
      SELECT 
        SUM(CASE WHEN docType IN ('Purchase Order', 'Proforma Invoice', 'Commercial Invoice') THEN 1 ELSE 0 END) AS documentCount,
        (SELECT COUNT(*) FROM tbl_shipment_booking_application WHERE bulkcreate IS NOT NULL) AS shipmentCount
      FROM tbl_sales_purchase_quotations
      WHERE contractId IS NOT NULL;
    `;

    const dbRes = await call({ query }, 'makeQuery', 'get');

    const { documentCount = 0, shipmentCount = 0 } = dbRes.message[0] || {};

    res.send({
      success: true,
      message: {
        count: documentCount,
        shipmentCount: shipmentCount,
      },
    });
  } catch (error) {
    console.error("Error in getContractStats:", error);
    res.status(500).send({ success: false, message: "Internal server error" });
  }
};

exports.getViewDocument = async (req, res, next) => {
  try {
    
  console.log("req body-->>>>")
    let {docId} = req.body
   
    
   
    let  query = `SELECT * FROM tbl_sales_purchase_quotations WHERE tbl_sales_purchase_quotations.docId = '${docId}'`
    console.log(query ,"queyr ")
    let dbRes = await call({ query }, 'makeQuery', 'get');
    res.send({
      success: true,
      data:dbRes.message,
    })
  } catch (error) {
    console.log("errorincreateSalesPurchaseQuotation", error)
    res.send({
      success: false
    })
  }
}


