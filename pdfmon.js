var odb = require( './common/odb.js' ),
  log = require( './common/logger.js' ),
  pool = null;

startProcessing();



// Functions
//
//

// Do any startup / initialisation stuff
startProcessing() {

  log.i();
  log.i( '----- DLINK JDE PDF Queue Monitoring starting' ); 

  // Listen for terminate and execute a controlled exit if and when necessary
  process.on( 'SIGTERM', controlledExit );

  // Start the monitoring process
  beginMonitoring();

}


// 
beginMonitoring() {

  odb.createPool( processWithPool )

}


//
function processWithPool() {

  if ( err ) {

    log.e( 'Failed to create an Oracle DB Connection Pool will retry shortly' );
    
 
  }

}


// Begin the monitoring process which will run continuosly
oraclebeginMonitoring() {



}



// Release any oracle database resources then exit
function controlledExit() {

  log.v( 'Process termination detected clean up and exit' );
  process.exit();

}




