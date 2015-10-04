var log = require( './common/logger.js' ),
  ondeath = require( 'death' )({ uncaughtException: true }),
  odb = require( './common/odb.js' ),
  pdfprocessqueue = require( './pdfprocessqueue.js' ),
  poolRetryInterval = 15000,
  pollInterval = 2000,
  dbp = null,
  monitorFromDate = null,
  monitorFromTime = null,
  lastJdeJob = null;


startUp();


// Functions
//
//

// Do any startup / initialisation stuff
function startUp() {

  log.i( '' );
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

    determineMonitorStartDateTime( dbp );
    
  }

}


// Monitoring should start from date and time of last entry to JDE Dlink Post PDF Handling Queue
// or Enterprise Server current System Date and Time - if the queue is empty (cleared down)
function determineMonitorStartDateTime( dbp ) {

  var workDateTime;

  // Only need to determine the Monitor From Date and Time first time this process runs
  // so if Monitor From Date and Time already set just continue on to Poll check step
  if ( monitorFromDate && monitorFromTime ) {

    pollJdePdfQueue( dbp ); 

  } else {

    pdfprocessqueue.getLatestQueueEntry( dbp, 
    function( err, result ) {

      if ( err ) {

        // Unable to determine Monitor Data and Time from Last Processed Monitored PDF entry so fallback
        // to current Oracle DB System Date and Time (AIX Date/Time)
        pdfprocessqueue.getEnterpriseServerSystemDateTime( dbp, 
        function( err, result ) {

          if ( err ) {

            log.d( 'Unable to determine Monitor start/date time from usual Oracle DB queries will wait and retry' );
            log.d( err );

            scheduleNextMonitorProcess( dbp );

          } else {

            // Unable to find Last entry in F559811 so use Oracle System Date/Time to start Monitoring from
            log.i( 'Monitoring will start from current Enterprise Server Date and Time: ' + result );
            
            pollJdePdfQueue( dbp );

          }
        });

      } else {

        // Found latest entry in F559811 JDE Post PDF Handling Process Queue - so start monitoring from there
        log.i( 'Monitoring will start from last PDF entry: ' + result );
        
        // Track last PDF procesed and its last Activity Date and Time i.e. when the UBE Job finished
        lastJdeJob = result[ 0 ];
        workDateTime = result[ 1 ].split(' ');
        monitorFromDate = workDateTime[ 0 ];
        monitorFromTime = workDateTime[ 1 ];

        pollJdePdfQueue( dbp );

      }
    });
  }

}


// Begin the monitoring process which will run continuously until server restart or docker stop issued
function pollJdePdfQueue( dbp ) {

  log.v( 'Last JDE Job was ' + lastJdeJob + '- now checking from ' + monitorFromDate + ' ' + monitorFromTime );

  //  queryJdeJobControl
  scheduleNextMonitorProcess( dbp );

}


function scheduleNextMonitorProcess( dbp ) {

  var cb;

  log.d( 'Next check in : ' + pollInterval + ' milliseconds' );
 
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





