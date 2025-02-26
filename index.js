'use strict';

// ----- Config [ start ] -----
const config = require('./config/app/config.json');
require('./utils/errors');
require('./utils/codeConfig');
var admin = require("firebase-admin/app");
const cron = require('node-cron');
var serviceAccount = require("./ServiceAccountCreds.json");

admin.initializeApp({
  credential: admin.cert(serviceAccount)
});
// ----- Config [ start ] -----

// ----- Logging setup [ start ] -----
const logger = require('./utils/logger').getLogger("FOB Client");
// ----- Logging setup [ end ] -----

// ----- server setup - requires [ start ] -----
var express = require('express');
const fileUpload = require('express-fileupload');
var cors = require('cors');
const fs = require('fs-extra');
const auth = require('./webApp/auth');
// ----- server setup - requires [ end ] -----

// -------- Creating required folder [ Start ]-------------------
fs.ensureDirSync('./docs');
fs.ensureDirSync('./outputDoc');
// -------- Creating required folder [ End ]-------------------


// ----- Middleware [ start ] -----
var app = express();
var http = require('http');
const server = http.Server(app);
const compression = require('compression');

app.options('*', cors());
app.use(cors());
app.use(compression())

app.use((req, res, next) => {
	logger.debug(req.originalUrl);
	next();
})
app.use(cors({
	origin: [config.cors.origin,config.cors2.origin],
	credentials: config.cors.credentials
}));
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: '50mb', extended: true }));
app.use(express.static(__dirname + '/public'));
app.use(fileUpload({
	extended: true
}));

const requestMiddleware = require('./src/middleware/request');
app.use(requestMiddleware.setRequestUUID);
const routes = require('./src/app');
const { call } = require('./utils/codeHelper');
const { encryptData, decryptData, sleep, environment, emailEnabledBanks, enabledFinanciersForLC } = require('./urlCostants');
const { dbPool } = require('./src/database/mysql');
const request = require('request');
const { insertMessage } = require('./utils/insertMessage');
const { sendFinanceNotification } = require('./controllers/lcFast/lcFast');
const { cronJobToUpdateDealStatus, tryToSendBuyerToStennIfNotDoneAlready, checkForInvoiceStatusOfStenn } = require('./src/cronjobs/stenn');
const { cronJobToUpdateModifiDealStatus, cronJobToUpdateRemarksFromModifi } = require('./src/cronjobs/modifi');
const {paymentReminders} = require('./src/cronjobs/paymentReminders')
const { adjustWalletBalance } = require('./controllers/walletManagement/walletManager');
const { applyForSBLCWhenAllDealsRejected } = require('./controllers/lcFastV2/lcFastV2');
const { update_wallet } = require('./src/cronjobs/wallet');
const { connectToEmailServer } = require('./controllers/processEmail/readAndProcessEmail');
const { sendQuoteDetailsAndMailToFinancer, applyForBuyerLimit } = require('./controllers/buyersDetailComp/buyersDetail');
const { sentTodaysDealToStenn, sendEmailToUserWhenStennGivesFinalLimit } = require('./controllers/finance/getDiscounting');
const { restoreFinanceLimitOnceDisbursed, pullInvoiceStatusFromModifi, skipTermSheetPartForStenn, syncInvoiceChargesAndCommission } = require('./controllers/finance/invoiceDiscountingWithoutSign');
const { mysqlJsonToText, removeNextLine, setDefaultCPCommission, convertValueIntoUSD, mysqlTextParse, formatSqlQuery, apiCall, jsonStr } = require('./iris_server/utils');
const { cronjobToFetchUpdatedBuyerQuote, getPotentialLimitFunc } = require('./controllers/apiComp/stennAPI');
const { update_plans } = require('./src/cronjobs/subscriptions');
const { syncUsersOnBlockchain } = require('./controllers/blockchainCrons/blockchainCrons');
const mongoose = require('mongoose');
app.use(routes);
const {fetchFormatsaveDataInDB, updateTargetViewTable} = require('./controllers/ReportsTally/TallyReports')
const moment = require("moment");
const { updateShipmentSummaryOfAllBuyers, fetchUserBuyerDetailsFromTTVData } = require('./src/cronjobs/shipmentData');
const { generateInvoiceBills, generateInvoiceBillsForCP } = require('./controllers/adminModules/AdminPayments');
const { syncLatestCurrencyRate, convertInvoiceTableContractAmount } = require('./src/cronjobs/currencyExchange');
const { getAddressComponents, getIECDetails } = require('./controllers/registrationControllers/registrationNew');
const { id } = require('./src/database/maps/plan');
const { MongoClient } = require('mongodb');
const { insertAPIsDataInMongo } = require('./controllers/openAPI/openAPI');
const CRMTasks = require('./src/database/Models/CRMTasksModel');
const ExporterModel = require('./src/database/Models/ExporterModel');
const { assignTasksToSubAdmins, dailyTaskReports } = require('./src/cronjobs/TaskAssignments');
const csvParser = require('csv-parser');
const TTV = require('./src/database/Models/TTVModel');
const { syncTradeFinanceNews, tradeFinanceNewsScrapper } = require('./controllers/GenericFeatures/GenericFeatures');
require('./webApp/public')(app);

app.use(auth.tokenAuth)

const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// middleware to verify the route access for a user

// app.use(auth.checkRoutes)

//  ----- Middleware [ end ] -----


// ----- route - [ start ] ----------
require('./webApp/router')(app);
require('./iris_server').initSocket(server)
const csv = require('csv-parser');
const ExporterModelV2 = require('./src/database/Models/ExporterModelV2');
const CRMTasksLogs = require('./src/database/Models/CRMTaskLogs');
const TTVModelV2 = require('./src/database/Models/TTVModelV2');
const { tryToSendBuyerToStennInBulkIfFailedToOnboard } = require('./src/genericScripts/stennScripts');
const { arrangeNavBarBasedOnClicks } = require('./src/genericScripts/scriptForNavBar');
const { modifyCompanyString } = require('./controllers/Admin/utils');
const { financierNotifyForDealFolloup } = require('./src/cronjobs/financierNotifyForDealFolloup');
const { removeDrafts, archiveOldPrivateChats } = require('./src/cronjobs/removeDrafts');
const { sendMailForDigitalSignWhenDocUpload } = require('./controllers/newDigitalSign/newDigitalSign');
const { syncTallyData } = require('./src/cronjobs/tallyDataHandling');
const { watsappMessage } = require('./utils/mailer');
const path = require('path');
var prompt = require('prompt-sync')();

// ----- server setup - configuration [ start ] -----
const port = config.port || "3212";
// const host = config.host || "localhost";
server.listen(port, () => {
	logger.info('[+] Listening @ ' + port);
})
// ----- server setup - configuration [ end ] -----

// fetchFormatsaveDataInDB()
// ------------ process error handling [ start  ]  ------

process.on('uncaughtException', err => {
	logger.error("'uncaughtException' occurred!")
	logger.error("err", err);
});

// eslint-disable-next-line no-unused-vars
process.on('unhandledRejection', (reason, promise) => {
	logger.error('Unhandled Rejection at:', reason.stack || reason);
});






const updateOnboardedToCRM =async () => {
	try{
		const query = `SELECT
    tbl_user_details.company_name,
    tbl_user.LeadAssignedTo,
    tbl_user.SecondaryLeadAssignedTo,
    tbl_user.id,
    tbl_user.type_id
FROM
    tbl_user
LEFT JOIN tbl_user_details ON tbl_user_details.tbl_user_id = tbl_user.id
WHERE
    tbl_user.type_id = 19 
GROUP BY
    tbl_user_details.company_name;`
		const dbRes = await call({query},'makeQuery','get')
		const ttvRes = dbRes.message
		const EXPORTER_NAMES = ttvRes
		//console.log('updateddd',updated.modifiedCount);
		let updatedCnt = 0
	 	for(let i = 0; i<=EXPORTER_NAMES.length - 1 ;i++){
			let element = EXPORTER_NAMES[i]
			let leadid = ''
			let LeadAssignedObj = []
			if(element.LeadAssignedTo && element.SecondaryLeadAssignedTo){
				leadid = `('${element.LeadAssignedTo}', '${element.SecondaryLeadAssignedTo}')`
			}else if(element.LeadAssignedTo){
				leadid = element.LeadAssignedTo
			}else{
				leadid = ''
			}
			if(leadid){
				const query = `SELECT tbl_user_id as id,contact_person,name_title,designation,email_id FROM tbl_user_details WHERE ${leadid.includes("(") ? `tbl_user_id IN ${leadid}` : `tbl_user_id = '${leadid}'`}`
				const dbRes = await call({query},'makeQuery','get')
				LeadAssignedObj = dbRes.message
			}
			const exporterlistres = await ExporterModelV2.updateOne({
				EXPORTER_NAME:modifyCompanyString(element.company_name)
			},{$set : {
				STATUS:4,
				tbl_user_id:element.id,
				TASK_ASSIGNED_TO: LeadAssignedObj
			}})
			updatedCnt += exporterlistres.modifiedCount	
		}
		console.log('doone',updatedCnt);

	}catch(e){
		console.log('error in apiress',e);
	}
}




if(config.isActualServer){
paymentReminders()
	syncTallyData()	
	sendMailForDigitalSignWhenDocUpload()
	removeDrafts()
	financierNotifyForDealFolloup()
	cronJobToUpdateRemarksFromModifi()
	// arrangeNavBarBasedOnClicks()
	assignTasksToSubAdmins()
	dailyTaskReports()
	updateShipmentSummaryOfAllBuyers()

	if(environment != "local"){
		syncInvoiceChargesAndCommission()
		syncLatestCurrencyRate()

	}


if (environment === "prod") {
	// tryToSendBuyerToStennInBulkIfFailedToOnboard() // Incase Failed to onboard error occurs
	generateInvoiceBills()
	generateInvoiceBillsForCP()
	// tryToSendBuyerToStennIfNotDoneAlready() // To update rejection reason why buyer was not added to stenn & update remarks in quote
	// cronJobToUpdateDealStatus() // Only for buyer noa & repaid status update
	// cronJobToUpdateModifiDealStatus()
	sentTodaysDealToStenn()
	// cronJobToUpdateDealStatusV2()
	checkForInvoiceStatusOfStenn()
	syncTradeFinanceNews()
}

//update_wallet()
// const cronSchedule = '0 11 * * *'; // Runs at 11 AM every day
// cron.schedule(cronSchedule,assignTasksToSubAdmins)
update_plans()
connectToEmailServer()
// sendQuoteDetailsAndMailToFinancer()
// if(environment === "dev"){
// 	syncUsersOnBlockchain()
// }
// applyForSBLCWhenAllDealsRejected() // Not required dont uncomment
restoreFinanceLimitOnceDisbursed()
if(environment!="local"){
	pullInvoiceStatusFromModifi()
	skipTermSheetPartForStenn()
}
}