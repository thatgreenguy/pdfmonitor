var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  log = require( './common/logger.js' ),
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };
  

module.exports.getFirstActiveJob = function(  pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = {},
    row,
    blkk,
    wka;

  log.d( 'Get Connection to find First Current PDF Job Running, Queued or Waiting' );

  oracledb.getConnection( credentials, function( err, dbc ) {

    if ( err ) {
      log.e( ' Unable to get Jde DB connection : ' + err );     
      return cbWhenDone( err );
    }  

    sql = "SELECT jcfndfuf2, jcsbmdate, jcsbmtime FROM " + jdeEnvDbF556110.trim() + ".F556110 WHERE jcjobsts in ('P', 'S', 'W') ORDER BY jcsbmdate, jcsbmtime ";
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

        pargs.monitorFromDate = row[ 1 ];
        pargs.monitorFromTime = row[ 2 ];
        log.v( 'Active or Queued Jobs Detected - First of which is : ' + row );

      } else {

        log.d( 'No Active or Queued Jobs detected.' );

      }
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
