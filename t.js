var async = require( 'async' ),
  moment = require( 'moment' ),
  log = require( './common/logger.js' ),
  audit = require( './common/audit.js' ),
  getlastpdf = require( './getlastpdf.js' ),
  getnewpdf = require( './getnewpdf.js' ),
  addnewpdf = require( './addnewpdf.js' ),
  pdfinqueue = require( './pdfinqueue.js' ),
  getjdedatetime = require( './getjdedatetime.js' ),
  pollInterval = process.env.POLLINTERVAL,
  monitorTimeOffset = 60;


// Continuously monitor JDE Job Control table for new Pdf entries and add them to post pdf process queue if required 
async.forever( check, error );


function check( cbDone ) {

  var parg = {},
    checkStart,
    checkEnd;

  checkStart = moment();
  log.d( ' Perform Check ( every ' + pollInterval + ' milliseconds )' );

  async.series([
    function( next ) { checkGetLastPdf( parg, next )  },
    function( next ) { checkGetJdeDateTime( parg, next )  },
    function( next ) { checkSetMonitorFrom( parg, next )  },
    function( next ) { checkGetNewPdf( parg, next )  }
  ], function( err, res ) {

    checkEnd = moment();
    if ( err ) {

      log.e( 'Unexpected error during check - Took : ' + moment.duration( checkEnd - checkStart ) );
      log.e( 'Unexpected error during check : ' + err );
      setTimeout( cbDone, pollInterval );
      
    } else {

      log.i( 'Check Complete : Added ' + parg.pdfAddCount + ' new PDF entries to Queue : Took : ' + moment.duration( checkEnd - checkStart) );  
      if ( parg.pdfAddErrorCount > 0 ) {
        log.i( 'Check Complete : Failed to Add ' + parg.pdfAddErrorCount + ' PDF entries to Queue - already added?' );  
      }
      setTimeout( cbDone, pollInterval );

    }
  });

}


function error( cbDone ) {

  log.e( ' Unexpected Error : ' + err );
  setImmediate( cbDone );

}


function checkGetLastPdf( parg, next ) {

  getlastpdf.getLastPdf( parg, function( err, result ) {

    if ( err ) {
      return next( err );
    }

    log.v( 'Last PDF processed : ' + result );
    parg.lastPdfRow = result;
    return next( null );

  });
}    


function checkGetJdeDateTime( parg, next ) {

  getjdedatetime.getJdeDateTime( parg, function( err, result ) {

    if ( err ) {
      return next( err );
    }

    log.v( 'Current JDE System Date/Time : ' + result );
    return next( null );

  });
}    


function checkSetMonitorFrom( parg, next ) {

  var jdeMoment;

  // Monitoring of the JDE job Control table is done from a particular Date and Time.
  // Usually the last PDF added to the process queue (F559811) determines this date and time
  // Idea is that as each new PDF is added to the process queue then the monitor query checks from that point forwards (keeps the query light)
  // However, if the F559811 is cleared (or empty on first run) then as fallback use the current JDE System Date and Time as the start point for monitoring
  // Once a new PDF is detected and added to the process queue then monitoring will continue from that point

  if ( parg.monitorFromDate === 0 ) {

    log.i( 'Last PDF check did not manage to set Monitor From Date and Time - F559811 file empty/cleared?' );
    log.i( 'As fallback - start monitoring from current AIX (JDE System) Date and Time - until next PDF added to F559811 Process Queue' );

    // Save AIX (JDE) Current System Date and Time in human readable format then convert monitor from date/time to JDE format
    // Factor in a safety offset window of 60 seconds as sometimes monitoring query runs just before trigger data is copied from F986110 to F556110
    parg.aixDateTime = parg.jdeDate + ' ' + parg.jdeTime;
    jdeMoment = moment( parg.aixDateTime ).subtract( monitorTimeOffset, 'seconds' );
    parg.monitorFromDate = audit.getJdeJulianDateFromMoment( jdeMoment );
    parg.monitorFromTime = jdeMoment.format( 'HHmmss' );

  }

  log.v( 'Monitor for new PDF entries from : ' + parg.aixDateTime + ' JDE style : ' + parg.monitorFromDate + ' ' + parg.monitorFromTime );
  return next( null );

}    


function checkGetNewPdf( parg, next ) {

  parg.pdfAddCount = 0;
  parg.pdfAddErrorCount = 0;

  getnewpdf.getNewPdf( parg, function( err, result ) {

    if ( err ) {
      return next( err );
    }

    log.v( 'New PDF entries : ' + parg.newPdfRows );

    async.eachSeries( 
      parg.newPdfRows, 
      function( row, cb ) {
      
        log.d( 'Row: ' + row );
        parg.checkPdf = row[ 0 ];
        parg.newPdfRow = row;


        pdfinqueue.pdfInQueue( parg, function( err, result ) {

          if ( err ) {

            log.e( parg.checkPdf + ' Error - Unable to verify if in Queue or not ' );
            return cb( err );

          } else {

            if ( parg.pdfInQueue >= 1 ) {
              log.d( parg.checkPdf + ' PDF already in Queue - Ignore it ' );
              return cb( null );

            } else {
              log.d( parg.checkPdf + ' PDF is new add to JDE Process Queue ' );

              addnewpdf.addNewPdf( parg, function( err, result ) {

                if ( err ) {

                  parg.pdfAddErrorCount += 1;
                  log.e( row[ 0 ] + ' : Failed to Add to Jde Process Queue : ' + err );
                  return cb( null);            

                } else {

                  parg.pdfAddCount += 1;
                  log.i( row[ 0 ] + ' : New PDF Added to Jde Process Queue ' );
                  return cb( null );
                }      
              });
            }        
          }
        });
     },
      next );
  });
}    
