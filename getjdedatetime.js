var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  log = require( './common/logger.js' ),
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };
  

module.exports.getJdeDateTime = function(  pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = {},
    row,
    systemdate,
    wka;

  log.d( 'Get Connection to query oracle DB for Aix (JDE) Current Date and Time' );

  oracledb.getConnection( credentials, function( err, dbc ) {

    if ( err ) {
      log.e( ' Unable to get Jde DB connection : ' + err );     
      return cbWhenDone( err );
    }  

    sql = 'SELECT TO_CHAR(SYSDATE, ';
    sql += "'" + 'YYYY-MM-DD HH24:MI:SS' + "'" + ') "NOW" FROM DUAL ';

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
      systemdate = row[ 0 ];
      if ( typeof systemdate !== 'undefined' ) {
        wka = systemdate.split(' ');
        pargs.jdeDate = wka[ 0 ];
        pargs.jdeTime = wka[ 1 ];
      }
      log.d( 'Current AIX (JDE) System Date and Time : ' + row );
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
