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
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  emailConfig = [];


// Fetch default email configuration for given Jde report name.
module.exports.fetchMailDefaults = function( dbCn, reportName, reportVersion ) {

  // If valid DB connection passed then use that and continue otherwise 
  // get new connection first
log.debug( dbCn );
log.debug( typeof( dbCn ) );
  if ( dbCn && dbCn !== 'null' && dbCn !== 'undefined' ) {

    log.debug( 'Connection pased through.' );
    connection = dbCn;
    queryJdeEmailConfig( connection, reportName, reportVersion );

  } else {

    log.debug( 'Establish connection first.' );
    oracledb.getConnection( credentials, function(err, connection) {
      if ( err ) { 
        log.error( 'Oracle DB Connection Failure' );
        return;
      }

      queryJdeEmailConfig( connection, reportName, reportVersion );

    });
  }
}


// Query the Jde Email Configuration Setup for this Report / Version.
function queryJdeEmailConfig( connection, reportName, reportVersion ) {

  var query;
  log.debug( 'Fetch email config for Report: ' + reportName + ' version: ' + reportVersion);
  if ( ! reportVersion ) {

    log.debug( 'Report Version not passed / defined so defaulting to "*ALL" ' );
    reportVersion = '*ALL';

  }

  query = "SELECT * FROM testdta.F559890 WHERE CRPGM = '" + reportName;
  query += "' AND CRVERNM = '" + reportVersion + "'";
  query += " AND CRCFGSID = 'PDFMAILER'";
  log.debug( query ); 

  connection.execute( query, [], { resultSet: true }, function( err, rs ) {
    if ( err ) {
      log.error( 'queryJdeEmailConfig Failed' );
      log.error( err.message );
      return;
    }
    
    processResultsFromF559890( connection, rs.resultSet, 1 );     

  });
}


// Process results of query on F559890 Jde Email Config Setup
function processResultsFromF559890( connection, rsF559890, numRows ) {

  var emailConfigRecord;

  rsF559890.getRows( numRows, function( err, rows ) {
    if ( err ) {
      oracleResultSetClose( connection, rsF559890 );
      log.verbose( 'No email configuration found' );

      // Error so return nothing much
      return null;

    } else if ( rows.length == 0 ) {
      
      oracleResultSetClose( connection, rsF559890 );
      log.debug( 'Finished processing email configuration entries' );

      // Done processing so return results
      return emailConfig;

    } else if ( rows.length > 0 ) {
 
      emailConfigRecord = rows[ 0 ];
      log.debug( 'Email Config entry:' );
      log.debug( emailConfigRecord );

      // Process the Email Configuration record entry
      processEmailConfigEntry(  connection, emailConfigRecord );

      // Fetch next Email config entry
      processResultsFromF559890( connection, rsF559890, 1 );     
      
    }
  });
}


// Process each Email Configuration entry
function processEmailConfigEntry( connection, emailConfigRecord ) {

  emailConfig.push( [emailConfigRecord[ 3 ], emailConfigRecord[ 5 ] ] );
  log.debug( 'Email Config List: ' + emailConfig );

}



// Close Oracle Database result set
function oracleResultSetClose( connection, rs ) {

  rs.close( function( err ) {
    if ( err ) {
      log.error( 'Failed to close/cleanup DB resultset' );
      log.error( err );
      oracleDbConnectionRelease( connection );
    }
  });
}


// Close Oracle Database result set
function oracleResultSetClose( connection ) {

  connection.release( function( err ) {
    if ( err ) {
      log.error( 'Failed to release/cleanup DB connection' );
      log.error( err );
      return;
    }
  });
}
