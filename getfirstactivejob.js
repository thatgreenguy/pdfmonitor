var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  log = require( './common/logger.js' ),
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  excludeSubsystemJobs = process.env.EXCLUDESUBSYSTEMJOBS,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };
  

module.exports.getFirstActiveJob = function(  pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = {},
    row,
    blkk,
    wka,
    jdeEnvCheck,
    excludedJobList,
    excludedJobSql;

  pargs.workingDate = 0;
  pargs.workingTime = 0;

  log.d( 'Get Connection to find First Current PDF Job Running, Queued or Waiting' );

  oracledb.getConnection( credentials, function( err, dbc ) {

    if ( err ) {
      log.e( ' Unable to get Jde DB connection : ' + err );     
      return cbWhenDone( err );
    }  

    // Process expects a JDE environment to be specified via environment variables so post PDF processing can be 
    // isolated for each JDE environment DV, PY, UAT and PROD
    // therefore environment check needs to be part of query restrictions so construct that here
    if ( jdeEnv === 'DV812' ) {
      jdeEnvCheck = " AND jcenhv IN ('DV812', 'JDV812') "; 
    } else {
      if ( jdeEnv === 'PY812' ) {
        jdeEnvCheck = " AND jcenhv IN ('PY812', 'JPY812', 'UAT812', 'JUAT812') "; 
      } else {
        if ( jdeEnv === 'PD812' ) {
          jdeEnvCheck = " AND jcenhv IN ('PD812', 'JPD812') ";      
        }
      }
    }

    // Exclude any JDE subsystem jobs from Active Job check as they can run for days or weeks at a time
    if ( typeof excludeSubsystemJobs !== 'undefined' ) {
      excludedJobList = excludeSubsystemJobs.split( " " );
      excludedJobSql = '';
      for ( var i = 0; i < excludedJobList.length; i++ ) {
        if ( excludedJobList[ i ].trim() !== '' ) {
          excludedJobSql += " AND jcfndfuf2 not like '" + excludedJobList[ i ].trim() + "%' ";
        }
      }
      sql = "SELECT jcfndfuf2, jcsbmdate, jcsbmtime FROM " + jdeEnvDbF556110.trim() + ".F556110 WHERE jcjobsts in ('P', 'S', 'W') " + jdeEnvCheck + excludedJobSql + " ORDER BY jcsbmdate, jcsbmtime ";
    } else {
      sql = "SELECT jcfndfuf2, jcsbmdate, jcsbmtime FROM " + jdeEnvDbF556110.trim() + ".F556110 WHERE jcjobsts in ('P', 'S', 'W') " + jdeEnvCheck + " ORDER BY jcsbmdate, jcsbmtime ";
    }

    log.d( sql );

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

        pargs.workingDate = row[ 1 ];
        pargs.workingTime = parseInt( row[ 2 ] );
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




