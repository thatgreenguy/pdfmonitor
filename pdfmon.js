var log = require( './common/logger.js' ),
  ondeath = require( 'death' )({ uncaughtException: true }),
  odb = require( './common/odb.js' ),
  pdfprocessqueue = require( './pdfprocessqueue.js' ),
  poolRetryInterval = 15000,
  pollInterval = 2000,
  dbp = null;


startUp();


// Functions
//
//

// Do any startup / initialisation stuff
function startUp() {

  log.i();
  log.i( '----- DLINK JDE PDF Queue Monitoring starting' ); 

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

pdfprocessqueue.getLatestQueueEntry( dbp, processLatestQueueEntry );

//    pollJdePdfQueue( dbp );
    
  }

}

function processLatestQueueEntry( err, result ) {

  if ( err ) {

    log.e( 'Unable to get Latest processed PDF' + err );

  } else {

  log.d( 'Latest Queue Entry was: ' + result );
  
  }

}



// Begin the monitoring process which will run continuosly
function pollJdePdfQueue( dbp ) {

  queryJdeJobControl
  scheduleNextMonitorProcess( dbp );

}


function scheduleNextMonitorProcess( dbp ) {

  var cb;

  log.v( 'Next check in : ' + pollInterval );
 
  cb = function() { pollJdePdfQueue( dbp ) };
 
  setTimeout( cb, pollInterval );    
 

}





// EXIT HANDLING
//
// Release any oracle database resources then exit
function exitControlled( signal, err ) {

  if ( err ) {
   
    log.e( 'Received error from ondeath?' + err ); 

  } else {

    log.e( 'Node process has died or been interrupted - Signal: ' + signal );
    log.e( 'Normally this would be due to DOCKER STOP command or CTRL-C or perhaps a crash' );
    log.e( 'Attempting Cleanup of Oracle DB resources before final exit' );
  }
  
  // Release Oracle resources
  if ( dbp ) {

      odb.terminatePool( dbp, exitProcess );

  } else {

    exitProcess();

  }

}


function exitProcess() {

  dbp = null;
  process.exit();

}





