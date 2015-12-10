var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  moment = require( 'moment' ),
  log = require( './common/logger.js' ),
  audit = require( './common/audit.js' ),
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  monitorTimeOffset = 60;


  

module.exports.getCurrentAixDateTime = function(  pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = {},
    row,
    systemdate,
    wka,
    jdeMoment;

  pargs.workingDate = 0;
  pargs.workingTime = 0;

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

        // Save AIX (JDE) Current System Date and Time in human readable format then convert monitor from date/time to JDE format
        // Factor in a safety offset window of 60 seconds as noticed some weird time adjustments by system on job control records where it adds 
        // couple of seconds then removes them if no data selected also possible slight delay on trigger copy due to blob size?

        wka = systemdate.split(' ');
        pargs.aixDateTime = wka[ 0 ] + ' ' + wka[ 1 ];
        jdeMoment = moment( pargs.aixDateTime ).subtract( monitorTimeOffset, 'seconds' );
        pargs.workingDate = audit.getJdeJulianDateFromMoment( jdeMoment );
        pargs.workingTime = jdeMoment.format( 'HHmmss' );

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


