var odb = require( './common/odb.js' ),
  log = require( './common/logger.js' ),
  ondeath = require( 'death' )({ uncaughtException: true }),
  poolRetryInterval = 15000,
  pollInterval = 2000,
  dbp = null,
  processExitCount = 0;


startUp();


// Functions
//
//

// Do any startup / initialisation stuff
function startUp() {

  log.i();
  log.i( '----- DLINK JDE PDF Queue Monitoring starting' ); 

  // Listen for terminate and execute a controlled exit if and when necessary
  // process.on( 'SIGTERM', controlledExit );

  // Handle process exit from DOCKER STOP, system interrupts, uncaughtexceptions or CTRL-C 
  ondeath( exitControlled );

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
    pollJdePdfQueue( dbp );
    
  }

}

// Begin the monitoring process which will run continuosly
function pollJdePdfQueue( dbp ) {

  log.v( 'Polling...' );
  log.d( dbp ); 
  scheduleNextMonitorProcess( dbp );

}


function scheduleNextMonitorProcess( dbp ) {

  var cb;

  log.v( 'Next check in : ' + pollInterval );
 
  cb = function() { pollJdePdfQueue( dbp ) };
 
  setTimeout( cb, pollInterval );    
 

}


// Release any oracle database resources then exit
function exitControlled( signal, err ) {

  if ( processExitCount != 0 ) {

    // Assume problem attempting cleanup code and just get out
    exitProcess();

  } else {

    processExitCount++;

    if ( err ) {
   
      log.e( 'Received error from ondeath?' + err ); 

    } else {

      log.e( 'Node process has died or been interrupted - Signal: ' + signal );
      log.e( 'Normally this would be due to DOCKER STOP command or CTRL-C or perhaps a crash' );
      log.e( 'Attempting Cleanup of Oracle DB resources before final exit' );

      if ( dbp ) {

        odb.terminatePool( dbp, exitProcess );

      } else {

        ExitProcess();

      }
    }
  }

}


function exitProcess() {

  dbp = null;
  process.exit();

}





