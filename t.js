var async = require( 'async' ),
  log = require( './common/logger.js' ),
  getlastpdf = require( './getlastpdf.js' ),
  addnewpdf = require( './addnewpdf.js' ),
  pollInterval = process.env.POLLINTERVAL;

var dummy = 1;

// Continuously monitor JDE Job Control table for new Pdf entries and add them to post pdf process queue if required 
async.forever( check, error );


function check( cbDone ) {

  var parg = {};

  log.d( ' Perform Check ( every ' + pollInterval + ' milliseconds )' );

  async.series([
    function( next ) { checkGetLastPdf( parg, next )  },
    function( next ) { checkAddNewPdf( parg, next )  }
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
    parg.newPdfRow = [  dummy , '115323', '101112'];
dummy = dummy + 1;
    return next( null );

  });
}    


function checkAddNewPdf( parg, next ) {

    log.v( 'parg is : ' + parg );    


  addnewpdf.addNewPdf( parg, function( err, result ) {

    if ( err ) {
      return next( err );
    }

    log.v( 'Add New PDF processed : ' + result );    
    return next( null );

  }); 
}


