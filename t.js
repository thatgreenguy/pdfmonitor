var async = require( 'async' ),
  log = require( './common/logger.js' ),
  getlastpdf = require( './getlastpdf.js' ),
  getnewpdf = require( './getnewpdf.js' ),
  addnewpdf = require( './addnewpdf.js' ),
  pdfinqueue = require( './pdfinqueue.js' ),
  getjdedatetime = require( './getjdedatetime.js' ),
  pollInterval = process.env.POLLINTERVAL;

// Continuously monitor JDE Job Control table for new Pdf entries and add them to post pdf process queue if required 
async.forever( check, error );


function check( cbDone ) {

  var parg = {};

  log.d( ' Perform Check ( every ' + pollInterval + ' milliseconds )' );

  async.series([
    function( next ) { checkGetLastPdf( parg, next )  },
    function( next ) { checkGetJdeDateTime( parg, next )  },
    function( next ) { checkSetMonitorFrom( parg, next )  },
    function( next ) { checkGetNewPdf( parg, next )  }
  ], function( err, res ) {

    if ( err ) {

      log.e( 'Hit error during Check : ' + err );
      setTimeout( cbDone, pollInterval );
      
    } else {

      log.v( 'Check ran without error : ' );
      setTimeout( cbDone, pollInterval );

    }
  });

}


function error( cbDone ) {

  log.e( ' Error returned ' + err );
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

  // Monitoring of the JDE job Control table is done from a particular Date and Time.
  // Usually the last PDF added to the process queue (F559811) determines this date and time
  // Idea is that as each new PDF is added to the process queue then the monitor query checks from that point forwards (keeps the query light)
  // However, if the F559811 is cleared (or empty on first run) then as fallback use the current JDE System Date and Time as the start point for monitoring
  // Once a new PDF is detected and added to the process queue then monitoring will continue from that point

  if ( parg.monitorFromDate === 0 ) {

    log.i( 'Last PDF check did not manage to set Monitor From Date and Time - F559811 file empty/cleared?' );
    log.i( 'As fallback - start monitoring from current AIX (JDE System) Date and Time - until next PDF added to F559811 Process Queue' );

    parg.monitorFromDate = parg.jdeDate;
    parg.monitorFromTime = parg.jdeTime;

  }

  log.v( 'Monitor for new PDF entries from : ' + parg.monitorFromDate + ' ' + parg.monitorFromTime );
  return next( null );

}    


function checkGetNewPdf( parg, next ) {

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

                  log.e( row[ 0 ] + ' : Failed to Add to Jde Process Queue : ' + err );
                  return cb( null);            

                } else {

                  log.i( row[ 0 ] + ' : Added to Jde Process Queue ' );
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
