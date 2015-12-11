var testme = require( './setmonitorfromdatetime.js' ),
  log = require( './common/logger.js' );

var p= {};


testme.setMonitorFromDateTime( p, function( err, res ) {

  if ( err ) {

    log.e( 'Error: Test failed ' + err );

  } else {

    log.i( 'Test OK ' );
    log.i( 'DATE: ' + p.monitorFromDate );
    log.i( 'TIME: ' + p.monitorFromTime );

  }
});
