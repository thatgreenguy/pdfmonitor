var testme = require( './getmonitorfrom.js' ),
  log = require( './common/logger.js' );

var p= {};


testme.getMonitorFrom( p, function( err, res ) {

  if ( err ) {

    log.e( 'Error: Test failed ' + err );

  } else {

    log.i( 'Test OK ' );
    log.i( 'DATE: ' + p.monitorFromDate );
    log.i( 'TIME: ' + p.monitorFromTime );

  }
});
