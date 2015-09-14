// Module		: emailconfig.js
// Description		: Fetch JDE Report Email Configuration both default values and version overrides.
// Author		: Paul Green
// Dated		: 2015-09-14
//
// Module provides functions to fetch JDE Email configuration/setup for JDE UBE Reports.
// Function provided to fetch default email settings for Report (*ALL version)
// Function provided to fetch override email settings for Report and specific version e.g. ZJDE0001
// Function provided to return definitive report/version email settings combining default and override values
  

var oracledb = require( 'oracledb' ),
  log = require( './logger' ),
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };


// Fetch default email configuration for given Jde report name.
exports.fetchMailDefaults = function( dbCn, reportName, reportVersion, processEmailConfig ) {

  // If valid DB connection passed then use that and continue otherwise 
  // get new connection first
  if ( typeof( dbCn !== null ) ) then {

    queryJdeEmailConfig( dbCn, reportName, reportVerion );

  } else {

    oracledb.getConnection( credentials, function(err, connection) {
    if ( err ) { 
      logger.error( 'Oracle DB Connection Failure' );
      return;
    }

    queryJdeEmailConfig( connection, reportName, reportVersion );

  }
}


// Fetch default email configuration for given Jde report name.
function queryJdeEmailConfig( dbCn, reportName, reportVersion ) {

  var query;

  if ( typeof( reportVersion ) === 'undefined' | reportVersion === false ) {

    log.debug( 'Report Version not passed / defined so defaulting to "*ALL" ' );
    reportVersion = '*ALL';

  }

  query = 'SELECT * FROM testdta.F559890 WHERE CRPGM = "' + reportName '" AND CRVERNM = "' + reportVersion + '"';
 
  dbCn.execute( query, [], { resultSet: true }, function( err, rs ) {
    if ( err ) {
      logger.error( 'queryJdeEmailConfig Failed' );
      logger.error( err.message );
      return;
    }
    
    processResultsFromF559890( dbCn, rs.resultSet, 1, processEmailConfig );     

  });
}


// Close Oracle Database result set
function oracleResultSetClose( dbCn, rs ) {

  rs.close( function( err ) {
    if ( err ) {
      log.error( 'Failed to close/cleanup DB resultset' );
      log.error( err );
      oracleDbConnectionRelease( dbCn );
    }
  });
}


// Close Oracle Database result set
function oracleResultSetClose( dbCn ) {

  dbCn.release( function( err ) {
    if ( err ) {
      log.error( 'Failed to release/cleanup DB connection' );
      log.error( err );
      return;
    }
  });
}
