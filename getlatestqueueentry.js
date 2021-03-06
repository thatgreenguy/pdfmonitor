var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  log = require( './common/logger.js' ),
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };
  

module.exports.getLatestQueueEntry = function(  pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = {},
    row,
    blkk,
    wka;

  pargs.workingDate = 0;
  pargs.workingTime = 0;

  log.d( 'Get Connection to find Last PDF Job added to Queue' );

  oracledb.getConnection( credentials, function( err, dbc ) {

    if ( err ) {
      log.e( ' Unable to get Jde DB connection : ' + err );     
      return cbWhenDone( err );
    }  

    sql = "SELECT jpfndfuf2, jpblkk FROM " + jdeEnvDb.trim() + ".F559811 ORDER BY jpupmj DESC, jpupmt DESC ";
    dbc.execute( sql, binds, options, function( err, result ) {

      if ( err ) {
        log.e( ' Jde Db Query execution failed : ' + err );
        dbc.release( function( err ) {
          if ( err ) {
            log.e( ' Unable to release Jde Db connection : ' + err );
            return cbWhenDone( err );
          }
        });     
        return cbWhenDone( err );
      }  

      row = result.rows[ 0 ];
      if ( typeof row !== 'undefined' ) {
        blkk = row[ 1 ];
        wka = blkk.split(' ');
        pargs.workingDate = wka[ 0 ];
        pargs.workingTime = parseInt( wka[ 1 ] );
      }
      log.d( 'Last PDF In Process Queue : ' + row );
      dbc.release( function( err ) {
        if ( err ) {
          log.e( ' Unable to release Jde Db connection : ' + err );
          return cbWhenDone( err );
        }
        return cbWhenDone( null, row );
      });          
    });
  });

}
