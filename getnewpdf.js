var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  moment = require( 'moment' ),
  log = require( './common/logger.js' ),
  audit = require( './common/audit.js' ),
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };
  

module.exports.getNewPdf = function(  pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = {},
    row;

  log.d( 'Get Connection to query for any new PDF entries since last PDF Job added to Queue' );

  oracledb.getConnection( credentials, function( err, dbc ) {

    if ( err ) {
      log.e( ' Unable to get Jde DB connection : ' + err );     
      return cbWhenDone( err );
    }  

    sql = constructQuery( pargs.monitorFromDate, pargs.monitorFromTime );
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

      pargs.newPdfRows = result.rows;
      log.d( 'Read following rows from Jde Job Control : ' + result );
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



// Construct query which is suitable for monitor from date and time and considering
// change of day on both startup and crossing midnight boundary
function constructQuery( monitorFromDate, monitorFromTime ) {

  var query = null,
      currentMomentAix,
      jdeTodayAix,
      jdeEnvCheck;

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

  // We have monitorFromDate to build the JDE Job Control checking query, however, we need to also account for 
  // application startups that are potentially checking from a few days ago plus we need to account for when we are 
  // repeatedly monitoring (normal running mode) and we cross the midnight threshold and experience a Date change

  // Check the passed Monitor From Date to see if it is TODAY or not - use AIX Time not CENTOS
  currentMomentAix = moment(); 
  jdeTodayAix = audit.getJdeJulianDateFromMoment( currentMomentAix );

  log.d( 'Check Date is : ' + monitorFromDate + ' Current (AIX) JDE Date is ' + jdeTodayAix );

  if ( monitorFromDate == jdeTodayAix ) {

    // On startup where startup is Today or whilst monitoring and no Date change yet
    // simply look for Job Control entries greater than or equal to monitorFromDate and monitorFromTime
     
    query = "SELECT jcfndfuf2, jcactdate, jcacttime FROM " + jdeEnvDbF556110.trim() + ".F556110 ";
    query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' " + jdeEnvCheck;
    query += " AND jcactdate = " + monitorFromDate + ' AND jcacttime >= ' + monitorFromTime;
    query += " AND jcpswd in ( SELECT DISTINCT crpgm FROM " + jdeEnvDb.trim() + ".F559890 WHERE crcfgsid in ('PDFMAIL', 'PDFLOGO') )";
//    query += " AND jcfndfuf2 NOT in ( SELECT jpfndfuf2 FROM " + jdeEnvDb.trim() + ".F559811 ) ";
    query += " ORDER BY jcactdate, jcacttime";  

  } else {

    // Otherwise Startup was before Today or we have crossed Midnight into a new day so query needs to adjust
    // and check for records on both sides of the date change

    query = "SELECT jcfndfuf2, jcactdate, jcacttime FROM " + jdeEnvDbF556110.trim() + ".F556110 ";
    query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' " + jdeEnvCheck;
    query += " AND (( jcactdate = " + monitorFromDate + " AND jcacttime >= " + monitorFromTime + ") ";
    query += " OR ( jcactdate > " + monitorFromDate + " )) ";
    query += " AND jcpswd in ( SELECT DISTINCT crpgm FROM " + jdeEnvDb.trim() + ".F559890 WHERE crcfgsid in ('PDFMAIL', 'PDFLOGO') ) ";
//    query += " AND jcfndfuf2 NOT in ( SELECT jpfndfuf2 FROM " + jdeEnvDb.trim() + ".F559811 ) ";
    query += " ORDER BY jcactdate, jcacttime";

  }

  log.d( query );

  return query;

}
