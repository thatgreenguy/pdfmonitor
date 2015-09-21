// pdfmonitor.js  : Monitor JDE Print Queue for new arrivals taht are configured for Post PDF processing
//                  and add them to the F559810 Dlink Post PDF Handling Queue   
// Author         : Paul Green
// Date           : 2015-09-21
//
// Synopsis
// --------
//
// Performs high frequency monitoring of the JDE Job Control table for completing Jde jobs.
// When a job completes a check is made to see if it's configured for any post PDF processing such as Logo's or Email.
// If it is an entry is added for the Job to the F559811 Dlink Post PDF Handling Queue.
// Entries added in the Queue will be picked up and processed by PDFHANDLER or PDFMAILER applications
// This application only monitors and adds entries to the Queue.


var oracledb = require( 'oracledb' ),
  log = require( './common/logger.js' ),
  audit = require( './common/audit.js' ),
  pdfChecker = require( './pdfchecker.js' ),
  pollInterval =  2500,
  dbCn = null,
  dbCredentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  hostname = process.env.HOSTNAME,
  logLevel = process.env.LOG_LEVEL,
  lastPdf = null,
  checkDate = null
  checkTime = null;


// - Initialisation
//
// Set logging level for console output
if ( typeof(logLevel) === 'undefined' ) {
  logLevel = 'debug';
}
log.transports.console.level = logLevel;

// Expect a valid docker hostname which is used for lock control and log file entries
log.info( '' );
log.info( '---------- Dlink Report Mailer - Start Monitoring JDE PrintQueue Jobs ----------'  );

if ( typeof( hostname ) === 'undefined' || hostname === '' ) {
  log.error( 'pdfhandler.js expects environment variable HOSTNAME to be set by docker container' );
  log.error( 'pdfhandler.js has detected a serious error - aborting process!' );
  process.exit( 1 );

} else {

  // Get Oracle Db connection once then pass through to be re-used
  oracledb.getConnection( dbCredentials, function( err, cn ) {

    if ( err ) {
      log.error( 'Oracle DB connection Failure : ' + err );
      process.exit( 1 );
    } 

    // Save, pass and reuse the connection
    dbCn = cn;

    // Log process startup in Jde Audit table 
    audit.createAuditEntry( dbCn, 'pdfmonitor', 'pdfmonitor.js', hostname, 'Start JDE PrintQueue Monitor' );

    // On startup determine Date and Time of last processed file or if none use current Date and TimeWhen process start perform the polled processing immediately then it will repeat periodically
    audit.determineLastProcessedDateTime( err, dbCn, startMonitoring );   

  });
}


// - Functions
//
// startMonitoring()
// performPolledProcess()
// scheduleNextPolledProcess()


// On startup determine Date and Time of last processed report and continue processing from there
// If first time ever run or Audit Log file cleared then start monitoring from Now adjusted by Aix server time offset (if any)
function startMonitoring( err, data ) {

  if ( err ) {

    log.error( 'PDFMONITOR Startup failed : ' + err );
    process.exit( 1 );

  } else {

    checkDate = data.lastAuditEntryDate;
    checkTime = data.lastAuditEntryTime;
    lastPdf = data.lastAuditEntryJob;

    log.info( 'Begin Monitoring from : ' + checkDate + ' ' + checkTime + ' then every : ' + pollInterval + ' milliseconds, Aix Server Time Offset is: ' + audit.aixTimeOffset + ' minutes'); 

    performPolledProcess();

  }
}


// Initiates polled process that is responsible for monitoring for new Jde reports arriving in Print Queue
function performPolledProcess( ) {

  pdfChecker.queryJdeJobControl( dbCn, checkDate, checkTime, pollInterval, hostname, lastPdf, scheduleNextPolledProcess );

}

// Handles scheduling of the next run of this high frequency polled process 
function scheduleNextPolledProcess() {

  var ts,
    ats;

  // Check done so adjust Check Date and Time for next check - only interested in checking recently finished Jde jobs now and wat to keep the
  // Sql query/resultset as light as possible.
  // AIX Server time is not currently inline with all other servers which are synchronised by NTP - at time of writing it is behind by approx 3 minutes.
  // This situation should be rectified but in the meantime a server Time Offset value is used to ensure current CENTOS time is adjusted back enough to 
  // allow the next time check to actually pick up any recently completed Jde Jobs.

  ts = audit.createTimestamp();
  ats = audit.adjustTimestampByMinutes( ts );

  // Set check Date and Time for next scheduled process
  checkDate = ats.jdeDate;
  checkTime = ats.jdeTime;

  log.debug( 'Next Check in : ' + pollInterval + ' milliseconds, using Date: ' + checkDate + ' and time: ' + checkTime );
  
  setTimeout( function() { performPolledProcess( checkDate, checkTime ) }, pollInterval );

}

