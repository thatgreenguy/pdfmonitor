var log = require( './common/logger.js' ),
  ondeath = require( 'death' )({ uncaughtException: true }),
  moment = require( 'moment' ),
  odb = require( './common/odb.js' ),
  audit = require( './common/audit.js' ),
  pdfchecker = require( './pdfchecker.js' ),
  pdfprocessqueue = require( './pdfprocessqueue.js' ),
  poolRetryInterval = 30000,
  pollInterval = 5000,
  dbp = null,
  monitorFromDate = null,
  monitorFromTime = null,
  lastJdeJob = null,
  timeOffset = 0;


startMonitorProcess();


// Functions
//
// startMonitorProcess() 
// establishPool() 
// processPool( err, pool ) 
// calculateTimeOffset( dbp ) 
// determineMonitorStartDateTime( dbp ) 
// pollJdePdfQueue( dbp ) 
// scheduleNextMonitorProcess( dbp ) 
// endMonitorProcess( signal, err ) 
//

// Do any startup / initialisation stuff
function startMonitorProcess() {

  log.i( '' );
  log.i( '----- DLINK JDE PDF Queue Monitoring starting' ); 

  // Handle process exit from DOCKER STOP, system interrupts, uncaughtexceptions or CTRL-C 
  ondeath( endMonitorProcess );

  // First need to establish an oracle DB connection pool to work with
  establishPool();

}


// Establish Oracle DB connection pool
function establishPool() {

  odb.createPool( processPool );

}


// Check pool is valid and continue otherwise pause then retry establishing a Pool 
function processPool( err, pool ) {

  if ( err ) {

    log.e( 'Failed to create an Oracle DB Connection Pool will retry shortly' );
    
    setTimeout( establishPool, poolRetryInterval );    
 
  } else {

    log.v( 'Oracle DB connection pool established' );
    dbp = pool;

    calculateTimeOffset( dbp );
    
  }

}


// Calculate the time offset between Oracle DB Host (AIX) and this application host (CENTOS)
function calculateTimeOffset( dbp ) {

  var currentDateTime,
    centosMoment,
    aixMoment;

  pdfprocessqueue.getEnterpriseServerSystemDateTime( dbp, 
  function( err, result ) {

    if ( err ) {

      log.e( 'Unable to determine System Date and Time from Oracle DB Host' + err );
      unexpectedDatabaseError( err );

    } else {

      // We should have Date and Time returned in format YYYY-MM-DD HH:MM:SS
      currentDateTime = new Date();
      centosMoment = moment();
      aixMoment = moment( result[ 0 ] );
      timeOffset = centosMoment - aixMoment;

      log.d( 'CENTOS ' + centosMoment.format() );
      log.d( 'AIX ' + aixMoment.format() );
      log.d( moment.duration( centosMoment - aixMoment ).humanize() );

      if ( centosMoment > aixMoment ) {
     
        timeOffset = 0 - timeOffset;

      } 

      log.info( 'Oracle Database Host Date and Time is : ' + result );
      log.info( 'Application Host Date and Time is : ' + currentDateTime );
      log.info( 'Calculated Time Offset between hosts is : ' + timeOffset + ' milliseconds' +
                ' or approx ' + moment.duration( centosMoment - aixMoment ).humanize() );

      determineMonitorStartDateTime( dbp, centosMoment, aixMoment );
 
    }
  });

}


// We have Current System Date and Time from Oracle DB Host set and any time offset value we need to consider
// Check the F559811 DLINK Post PDF Handling Queue table for last processed entry - if found we need to check from
// this Date and Time to handle recovery if application has been down for a while - if not found will simply
// start monitoring from current System Date and Time passed thru...
function determineMonitorStartDateTime( dbp, centosMoment, aixMoment ) {

  var workDateTime;

  // On start up look at last processed entry and if found start monitoring from there.

    pdfprocessqueue.getLatestQueueEntry( dbp, function( err, result ) {

      if ( err ) {

        log.e( 'Unable to access the F559811 for last processed entry' + err );
        unexpectedDatabaseError( err );

      } else {

        // If table Empty then use System Date and Time from Oracle Host to begin Monitoring
        if ( result === null ) {

          lastJdeJob = 'unknown'; 
          monitorFromDate = audit.getJdeJulianDateFromMoment( aixMoment );
          monitorFromTime = aixMoment.format( 'HHmmss' );
          

        } else {

          // Track last PDF procesed and its last Activity Date and Time i.e. when the UBE Job finished
          lastJdeJob = result[ 0 ];
          workDateTime = result[ 1 ].split(' ');
          monitorFromDate = workDateTime[ 0 ];
          monitorFromTime = workDateTime[ 1 ];

        }

        // Found latest entry in F559811 JDE Post PDF Handling Process Queue - so start monitoring from there
        log.i( 'Begin Monitoring from : ' + monitorFromDate + ' ' + monitorFromTime );     

        pollJdePdfQueue( dbp );

      }
    });
}


// Begin the monitoring process which will run continuously until server restart or docker stop issued
function pollJdePdfQueue( dbp ) {

  var cb;

  // TESTING ---------------------------------
//  monitorFromDate = 115285;
//  monitorFromTime = 000000;

  log.v( 'Last JDE Job was ' + lastJdeJob + ' - Checking from ' + monitorFromDate + ' ' + monitorFromTime );

  cb = function() { scheduleNextMonitorProcess( dbp ) }; 
  pdfchecker.queryJdeJobControl( dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, 
    lastJdeJob, cb );
  
}


// When done processing any new PDF entries this is called to set up the next polled job 
function scheduleNextMonitorProcess( dbp ) {

  var cb;

  log.v('');

  cb = function() { calculateTimeOffset( dbp ) }; 
  setTimeout( cb, pollInterval );    
 

}


// Database error handling
// Call this whe unexpected DB error encountered
function unexpectedDatabaseError( err ) {

  log.e( 'Unexpected Database error - unsure how to recover from this so exiting now!' + err );
  releaseOracleResources( 1 ); 

}


// EXIT HANDLING
//
// Note: DOCKER STOP or CTRL-C is not considered a failed process - just a way to stop this application - so node exits with 0
// An uncaught exception is considered a program crash so exists with code = 1
function endMonitorProcess( signal, err ) {

  if ( err ) {
   
    log.e( 'Received error from ondeath?' + err ); 

    releaseOracleResources( 2 ); 


  } else {

    log.e( 'Node process has died or been interrupted - Signal: ' + signal );
    log.e( 'Normally this would be due to DOCKER STOP command or CTRL-C or perhaps a crash' );
    log.e( 'Attempting Cleanup of Oracle DB resources before final exit' );
  
    releaseOracleResources( 0 ); 

  }

}


// Check to see if database pool is valid and if so attempt to release Oracle DB resources back to the Database
// This function can be called from endMonitorProcess or if a database related error is detected
// If unable to release resources cleanly application will exit with non-zero code (connections not released correctly?) 
function releaseOracleResources( suggestedExitCode ) {

  log.e( 'Problem detected so attempting to release Oracle DB resources' );
  log.e( 'Application may exit or wait briefly and attempt recovery' );

  // If no exit code passed in default it to exit with 0
  if ( typeof( suggestedExitCode ) === 'undefined' ) { suggestedExitCode = 0 } 

  // Release Oracle resources
  if ( dbp ) {

    odb.terminatePool( dbp, function( err ) {

      if ( err ) {

        log.d( 'Failed to release Oracle DB Connection Pool resources: ' + err );

        dbp = null;
        process.exit( 2 );

      } else {

        log.d( 'Oracle DB Connection Pool resources released successfully: ' );

        process.exit( suggestedExitCode );

      }
    });

  } else {

    log.d( 'No Oracle DB Connection Pool to release: ' );

    dbp = null;
    process.exit( suggestedExitCode );

  }

}
