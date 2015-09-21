// pdfmonitor.js  : Monitor JDE Print Queue for new arrivals taht are configured for Post PDF processing
//                  and add them to the F559810 Dlink Post PDF Handling Queue   
// Author         : Paul Green
// Date           : 2015-09-21
//
// Synopsis
// --------
//
// Establish remote mount connectivity via sshfs to the Jde PrintQueue directory on the (AIX) Enterprise server
// Perform medium frequency polling of the Oracle (JDE) table which holds information on Jde UBE jobs
// When detecting new JDE PDF files that have been configured (in Jde) to be emailed out then handle email
// transmission 


var oracledb = require( 'oracledb' ),
  log = require( './common/logger.js' ),
  mounts = require( './common/mounts.js' ),
  audit = require( './common/audit.js' ),
  pdfChecker = require( './pdfchecker.js' ),
  pollInterval =  5000,
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
// performPostRemoteMountChecks( err, data )
// reconnectToJde( err )
// performPostEstablishRemoteMounts( err, data )


// On startup determine Date and Time of last processed report and continue processing from there
// If first time ever run or Audit Log file cleared then start from monitoring from Now
function startMonitoring( err, data ) {

  if ( err ) {

    log.error( 'PDFMONITOR Startup failed : ' + err );
    process.exit( 1 );

  } else {

    checkDate = data.lastAuditEntryDate;
    checkTime = data.lastAuditEntryTime;
    lastPdf = data.lastAuditEntryJob;

    log.info( 'Monitor for new Jde Jobs completing from : ' + checkDate + ' ' + checkTime + ' then every : ' + pollInterval + ' milliseconds, Aix Server Time Offset is: ' + audit.aixTimeOffset + ' minutes'); 

    // Startup has valid Date and Time to check from so perform checks then repeat periodically
    performPolledProcess();

  }
}


// Initiates polled process that is responsible for monitoring for new Jde reports arriving in Print Queue
function performPolledProcess() {

  // Check remote mounts to Jde Pdf files are working then process
  mounts.checkRemoteMounts( performPostRemoteMountChecks );

}

// Handles scheduling of the next run of the frequently polled process 
function scheduleNextPolledProcess() {

  var ts,
    ats;

  // Check done so adjust Check Date and Time for next check - only interested in new PDF files now
  ts = audit.createTimestamp();
  ats = audit.adjustTimestampByMinutes( ts );

  // Set check Date and Time for next scheduled process
  checkDate = ats.jdeDate;
  checkTime = ats.jdeTime;

  log.debug( 'Will Check again in : ' + pollInterval + ' milliseconds, using Date: ' + checkDate + ' time: ' + checkTime );
  setTimeout( performPolledProcess, pollInterval );

}


// Called after remote mounts to Jde have been checked
function performPostRemoteMountChecks( err, data ) {

  if ( err ) {

    // Problem with remote mounts so need to reconnect before doing anything else
    reconnectToJde( err );

  } else {

    // Remote mounts okay so go ahead and process, checking for new Pdf's etc
    pdfChecker.queryJdeJobControl( 
      dbCn, checkDate, checkTime, pollInterval, hostname, lastPdf, scheduleNextPolledProcess );
  }

}


// Problem with remote mounts to jde so attempt to reconnect 
function reconnectToJde( err ) {

    log.debug( 'Error data: ' +  err );
    log.warn( 'Issue with Remote mounts to JDE - Attempting to reconnect.' );

    mounts.establishRemoteMounts( performPostEstablishRemoteMounts );

}


// Called after establish remote mounts to Jde has been processed
function performPostEstablishRemoteMounts( err, data ) {

  if ( err ) {

    // Unable to reconnect to Jde at the moment so pause and retry shortly
    log.warn( '' );
    log.warn( 'Unable to re-establish remote mounts to Jde will pause and retry' );
    scheduleNextPolledProcess();

  } else {

    // Remote mounts okay so resume normal processing 
    log.verbose( 'Remote mounts to Jde re-established - will continue normally')
    pdfChecker.queryJdeJobControl( 
      dbCn, checkDate, checkTime, pollInterval, hostname, lastPdf, scheduleNextPolledProcess );

  }
}
