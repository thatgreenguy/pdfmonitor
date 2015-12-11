var async = require( 'async' ),
  log = require( './common/logger.js' ),
  getfirstactivejob = require( './getfirstactivejob.js' );
  getlatestqueueentry = require( './getlatestqueueentry.js' ),
  getcurrentaixdatetime = require( './getcurrentaixdatetime.js' );


// Need to set a sensible date and time from which to start checking for new PDF entries
// want to keep advancing this so query is light and fast but don't want to advance it too fast
// in case we miss a slow running or queued job - remember we are dealing with multi threaded job queues
// single threaded job queues and multiple jobs submitted by scheduler or manually at any one time
// 
// 1 - Check for any Active or Queued Jobs (running or Waiting)
//   - If found use earliest Date and Time otherwise
// 2 - Check last PDF processed in F559811 Queue
//   - If found use latest entry otherwise
// 3 - Get current Aix Date and Time and move backwards in time by a safety offset say 60 seconds
//   
module.exports.setMonitorFromDateTime = function( pargs, cbWhenDone ) {

  pargs.workingDate = 0;
  pargs.workingTime = 0;
  pargs.monitorFromDate = 0;
  pargs.monitorFromTime = 0;

  async.series([
    function( next ) { checkEarliestActiveJob( pargs, next ) },
    function( next ) { determineEarliestDateTime( pargs, next ) },
    function( next ) { checkLatestQueueEntry( pargs, next ) },
    function( next ) { determineEarliestDateTime( pargs, next ) },
    function( next ) { checkCurrentAixDateTime( pargs, next ) },
    function( next ) { determineEarliestDateTime( pargs, next ) }
  ], function( err, res ) {

    if ( err ) {

      return cbWhenDone( err );

    } else {

      if ( pargs.monitorFromDate == 0 ) {

        // Unable to determine a starting Date and Time for job Control Query check
        // report error and return with error - maybe network or DB down temporarily?
        log.w( 'Unable to determine sensible date and time to start Monitoring from - try again shortly' );
        return cbWhenDone( new Error('Unable to determine Monitor From Date and Time') ); 
   
      } else {

        log.i( 'Monitor from Date: ' + pargs.monitorFromDate + ' and Time: ' + pargs.monitorFromTime );
        log.i( ' ' );
        return cbWhenDone( null );

      }
    }
  });
} 


function checkEarliestActiveJob( pargs, next ) {

  getfirstactivejob.getFirstActiveJob( pargs, function( err, res ) {

    if ( err ) {

      log.e( 'Error : ' + err);
      return next( err );

    } else {

      return next ( null );

    } 
  });
}


function checkLatestQueueEntry( pargs, next ) {

  // Fetch latest queue entry (if there) setting monitor from Date/Time  
  getlatestqueueentry.getLatestQueueEntry( pargs, function( err, res ) {

    if ( err ) {

      log.e( 'Error : ' + err);
      return next( err );

    } else {

      return next ( null );

    } 
  });
}


function checkCurrentAixDateTime( pargs, next ) {

  // Fetch latest queue entry (if there) setting monitor from Date/Time  
  getcurrentaixdatetime.getCurrentAixDateTime( pargs, function( err, res ) {

    if ( err ) {

      log.e( 'Error : ' + err);
      return next( err );

    } else {

      return next ( null );

    } 
  });
}

// Each time we retrieve Monitor From Date/Time check to see if it is before or after
// previous date/time values and use earliest Date/Time to monitor From
// 
function determineEarliestDateTime( pargs, next ) {

  log.v( 'IN: Monitor From Date and Time: ' + pargs.monitorFromDate + ' ' + pargs.monitorFromTime + ' Working Date and Time: ' + pargs.workingDate + ' ' + pargs.workingTime );

  // If monitor From Date and Time not yet set then set them to current working values
  if ( pargs.monitorFromDate == 0 ) {

    pargs.monitorFromDate = pargs.workingDate;
    pargs.monitorFromTime = pargs.workingTime;  

  } else {

    // If working Date is set to seomthing then do comparison and swap values if required
    if ( pargs.workingDate != 0 ) {

      // If latest working date/time is earlier than current monitor From Date/Time then use earlier date/time values
      if ( pargs.workingDate < pargs.monitorFromDate ) {

        pargs.monitorFromDate = pargs.workingDate;
        pargs.monitorFromTime = pargs.workingTime;

      } else {

        // If Dates are same but latets working Time is earlier than current monitor From Time then use earlier values
        if ( pargs.workingDate == pargs.monitorFromDate && pargs.workingTime < pargs.monitorFromTime ) {

          pargs.monitorFromDate = pargs.workingDate;
          pargs.monitorFromTime = pargs.workingTime;

        }
      }
    }
  }

  log.v( 'OUT: Monitor From Date and Time: ' + pargs.monitorFromDate + ' ' + pargs.monitorFromTime );

  return next( null );

}





