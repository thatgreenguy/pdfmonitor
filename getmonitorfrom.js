var async = require( 'async' ),
  log = require( './common/logger.js' ),
  getfirstactivejob = require( './getfirstactivejob' );
  getlatestqueueentry = require( './getlatestqueueentry' );


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
module.exports.getMonitorFrom = function( pargs, cbWhenDone ) {

  pargs.tmpDate = 0;
  pargs.tmpTime = 0;
  pargs.monitorFromDate = 0;
  pargs.monitorFromTime = 0;

  async.series([
    function( next ) { checkCurrentJobs( pargs, next ) },
    function( next ) { determineBestDateTime( pargs, next ) },
    function( next ) { getLastPdfProcessed( pargs, next ) },
    function( next ) { determineBestDateTime( pargs, next ) },
    function( next ) { getCurrentAixDateTime( pargs, next ) },
    function( next ) { determineBestDateTime( pargs, next ) },
  ], function( err, res ) {

    if ( err ) {

      return cbWhenDone( err );

    } else {

      if ( pargs.monitorFromDate === 0 ) {

        // Unable to determine a starting Date and Time for job Control Query check
        // report error and return with error - maybe network or DB down temporarily?
        log.w( 'Unable to determine sensible date and time to start Monitoring from - try again shortly' );
        return cbWhenDone( new Error('Unable to determine Monitor From Date and Time') ); 
   
      } else {

        log.i( 'Monitor from Date: ' + pargs.monitorFromDate + ' and Time: ' + pargs.monitorFromTime );
        return cbWhenDone( null );

      }
    }
  });
} 


function checkCurrentJobs( pargs, next ) {

  getfirstactivejob.getFirstActiveJob( pargs, function( err, res ) {

    if ( err ) {

      log.e( 'Error : ' + err);
      return next( err );

    } else {

      log.v( 'Got result from Active jobs : ' + res);

      pargs.tmpDate = pargs.monitorFromDate;
      pargs.tmpTime = pargs.monitorFromTime;

      return next ( null );

    } 
  });
}


function getLastPdfProcessed( pargs, next ) {

  var saveDate, 
    saveTime;


    // Fetch latest queue entry (if there) setting monitor from Date/Time  
    getlatestqueueentry.getLatestQueueEntry( pargs, function( err, res ) {

      if ( err ) {

        log.e( 'Error : ' + err);
        return next( err );

      } else {

        log.v( 'Got result from latest queue entry : ' + res );

        pargs.tmpDate = pargs.monitorFromDate;
        pargs.tmpTime = pargs.monitorFromTime;

        return next ( null );

      } 
    });

  } else {

    return next ( null );

  }
}


function getCurrentAixDateTime( pargs, next ) {

  // If we have Monitor From date and Time already set then just return
  if ( pargs.monitorFromDate === 0 ) {

    return next ( null );

  } else {

    return next ( null );

  }
}

// Each time we retrieve Monitor From Date/Time check to see if it is before or after
// previous date/time values and use earliest Date/Time to monitor From
// 
function determineBestDateTime( pargs, next ) {

  // If Saved date is before latest retrieved monitor from date then use that
  if ( pargs.tmpDate < pargs.monitorFromDate ) {

    pargs.monitorFromDate = pargs.tmpDate;
    pargs.monitorFromTime = pargs.tmpTime;

  } else {

    // If saved Date is same but saved time is before retrieved monitor form time then use that
    if ( pargs.tmpDate == pargs.monitorFromDate && pargs.tmpTime < pargs.monitorFromTime ) {

      pargs.monitorFromDate = pargs.tmpDate;
      pargs.monitorFromTime = pargs.tmpTime;

    }
  }
}





