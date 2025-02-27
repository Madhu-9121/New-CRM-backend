exports.sendMail = function (mailOption, callback, loggedInUserId , ) {


    let error, response;
    const promiseToReturn = new Promise(async(resolve, reject) => {        
        if (environment === "prod" && !mailOption?.["replyTo"]?.length) {
            mailOption["replyTo"] = ["aman@tradereboot.com"]
        }
        // Add admin email in cc if it's his deal start
        let tempCCs = []
        for (let index = 0; index < mailOption?.to?.length; index++) {
            let element = mailOption?.to[index];
            let userQry = formatSqlQuery(`SELECT adminTblUser.login_id AS cc1, adminTblUser2.login_id AS cc2 FROM tbl_user 
            LEFT JOIN tbl_user AS adminTblUser ON
            adminTblUser.id = tbl_user.LeadAssignedTo
            LEFT JOIN tbl_user AS adminTblUser2 ON
            adminTblUser2.id = tbl_user.SecondaryLeadAssignedTo
            WHERE tbl_user.login_id = ? `, [element])
            let dbRes = await call({ query: userQry }, 'makeQuery', 'get')
            if(dbRes.message?.[0]?.cc1){
                tempCCs.push(dbRes.message[0].cc1)
            }
            if(dbRes.message?.[0]?.cc2){
                tempCCs.push(dbRes.message[0].cc2)
            }
        }
        if(tempCCs.length){
            if(!mailOption?.["cc"]?.length){
                mailOption["cc"] = []
            }
            mailOption["cc"] = mailOption["cc"].concat(tempCCs)
        }
        // Add admin email in cc if it's his deal end
        // console.log("mailOptions in sendMailFunc---->", mailOption)
        if (!mailOption.skipToSubUser && mailOption?.to?.length){
            const userQuery = `SELECT id FROM tbl_user WHERE login_id = '${mailOption?.to}'`
            const userRes = await call({query:userQuery},'makeQuery','get')
            if(userRes.message.length){
                const subUserQuery = `SELECT login_id FROM tbl_user WHERE parent_id = '${userRes.message[0].id}'`
                const subUserRes = await call({query:subUserQuery},'makeQuery','get')
                if(subUserRes?.message?.length){
                    mailOption["to"] = mailOption.to.concat(subUserRes.message.map(item => item.login_id))
                }
            }
        }
        // Check if mails are allowed to send or not start
        let sendTo = []
        for (let index = 0; index < mailOption?.to?.length; index++) {
            const element = mailOption?.to[index];
            let qrry = formatSqlQuery(`SELECT * FROM tbl_email_metadata WHERE email = ? AND stopMail = 1 `, [element, 1])
            let dbRes = await call({query:qrry},'makeQuery','get')  
            if(!dbRes.message.length){
                if(element!="support@tradereboot.com"){
                    sendTo.push(element)
                }
            }        
        }
        let sendCc = []
        for (let index = 0; index < mailOption?.cc?.length; index++) {
            const element = mailOption?.cc[index];
            let qrry = formatSqlQuery(`SELECT * FROM tbl_email_metadata WHERE email = ? AND stopMail = 1 `, [element, 1])
            let dbRes = await call({query:qrry},'makeQuery','get')  
            if(!dbRes.message.length){
                sendCc.push(element)
            }        
        }
        console.log("mailer111111111111111111111111111111111");
        let sendBcc = []
        for (let index = 0; index < mailOption?.bcc?.length; index++) {
            const element = mailOption?.bcc[index];
            let qrry = formatSqlQuery(`SELECT * FROM tbl_email_metadata WHERE email = ? AND stopMail = 1 `, [element, 1])
            let dbRes = await call({query:qrry},'makeQuery','get')  
            if(!dbRes.message.length){
                sendBcc.push(element)
            }        
        }
        // Check if mails are allowed to send or not end
        mailOption["to"] = sendTo
        mailOption["cc"] = sendCc
        mailOption["bcc"] = sendBcc

        let transporter = nodemailer.createTransport(mailConfig);

        // Check if admin user have saved authorized to send mail with their id & password start
        if(loggedInUserId){
            let userCredentialCheckResp = await call({query: formatSqlQuery(`SELECT tbl_admin_mail_credentials.* FROM tbl_user_details
            LEFT JOIN tbl_admin_mail_credentials ON tbl_user_details.email_id COLLATE utf8mb4_unicode_ci = tbl_admin_mail_credentials.mail COLLATE utf8mb4_unicode_ci
            WHERE tbl_user_details.tbl_user_id = ? AND tbl_admin_mail_credentials.mail IS NOT NULL`, [loggedInUserId])},'makeQuery','get')
            if(userCredentialCheckResp.message.length){   
                let tempMailConfig = {}
                if(userCredentialCheckResp.message[0].account==="gmail"){
                    tempMailConfig["service"] = config.mail.service
                    tempMailConfig["auth"] = {
                        user: userCredentialCheckResp.message[0].mail,
                        pass: decryptData(userCredentialCheckResp.message[0].password)
                    }
                }
                else if(userCredentialCheckResp.message[0].account==="hostinger"){
                    tempMailConfig = {
                        host: "smtp.hostinger.com",
                        port: 465,
                        auth: {
                          user: userCredentialCheckResp.message[0].mail,
                          pass: decryptData(userCredentialCheckResp.message[0].password)
                        }
                    }
                }
                else if(userCredentialCheckResp.message[0].account==="outlook"){
                    tempMailConfig = {
                        host: "smtp-mail.outlook.com",
                        port: 587,
                        auth: {
                          user: userCredentialCheckResp.message[0].mail,
                          pass: decryptData(userCredentialCheckResp.message[0].password)
                        }
                    }
                }
                transporter = nodemailer.createTransport(tempMailConfig)
                mailOption["from"] = userCredentialCheckResp.message[0].mail
            }
        } 
        // Check if admin user have saved authorized to send mail with their id & password end 
        // console.log("mailOptionssssssssssssssssssssssssssssssss", mailOption, transporter);

    //     for(let i = 0 ; i<mailOption?.to?.length ; i++){
    //     let query = `SELECT LeadAssignedTo, SecondaryLeadAssignedTo FROM tbl_user WHERE login_id = '${mailOption.to[i]}'`;
    //     let dbRes = await call({ query }, 'makeQuery', 'get');
    //    const lead1 = dbRes.message[0].LeadAssignedTo
    //    mailOption?.cc?.push(lead1)
    //    const lead2 = dbRes.message[0].SecondaryLeadAssignedTo
    //    mailOption?.cc?.push(lead2)  
    //     }
        
        console.log("mailOptions in sendMailFunc---->", mailOption);
        if(mailOption?.to?.length){
            transporter.sendMail(mailOption, function (err, info) {
                // console.log("sendmailresppp", err, info);
                if (err) {
                    error = err
                    return resolve(error);
                }
                else {
                    response = info
                    return resolve(response)
                }
            });
        }
    })
    if (callback && typeof callback) {
        promiseToReturn.then(callback.bind(null, null), callback)
    } else {
        return promiseToReturn
    }
}