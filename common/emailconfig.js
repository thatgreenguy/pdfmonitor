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
  async = require( 'async' ),
  log = require( './logger' ),
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };


// Functions  -
//
// module.exports.fetchMailDefaults = function( dbCn, reportName, reportVersion, cb )
// module.exports.mergeMailOptions = function( reportOptions, versionOptions, cb )
// function processVersionOverrides( reportOptions, versionOptions, mailOptions, versionOption, cb )
// function removeOverrideOption( reportOptions, versionOptions, mailOptions, versionOption, reportOption, cb )
// function queryJdeEmailConfig( connection, reportName, reportVersion, cb, emailConfig )
// function processResultsFromF559890( connection, rsF559890, numRows, cb, emailConfig )
// function processEmailConfigEntry( connection, emailConfigRecord, emailConfig )
// function oracleResultSetClose( connection, rs )
// function oracleResultSetClose( connection )



// Fetch default email configuration for given Jde report name.
module.exports.fetchMailDefaults = function( dbCn, jdeJob, reportName, reportVersion, cb ) {

  var emailConfig = [];

  // If valid DB connection passed then use that and continue otherwise 
  // get new connection first
  if ( dbCn && dbCn !== 'null' && dbCn !== 'undefined' ) {

    log.debug( 'Connection pased through.' );
    connection = dbCn;
    queryJdeEmailConfig( connection, jdeJob, reportName, reportVersion, cb, emailConfig );

  } else {

    log.debug( 'Establish connection first.' );
    oracledb.getConnection( credentials, function(err, connection) {
      if ( err ) { 
        log.error( 'Oracle DB Connection Failure' );
        return;
      }

      queryJdeEmailConfig( connection, jdeJob, reportName, reportVersion, cb, emailConfig );

    });
  }
}

// Multiple email options can be defined for a Report and any of those options can be overridden at report/version level.
// This function takes the options for the report and those for the version overrides (if any) and returns a merged set of 
// options.
// if same option is defined at version level as at report level then the report level option is completely 
// replaced by the version override.
// Otherwise the result is a combination of report and version specific options.
module.exports.mergeMailOptions = function( reportOptions, versionOptions, cb ) {

  var mailOptions = reportOptions.slice();

  // Show array before
  log.info( 'Before: ' + mailOptions )

  // Iterate over Version specific overrides and remove them from report mail options first
  async.each(
    versionOptions,
    async.apply( processVersionOverrides, reportOptions, versionOptions, mailOptions ),
    function ( err ) {
      if ( err ) {
        log.error( 'mergMailOptions encountered error' );
        log.error( err );
        cb( err, [] );     
      }    
  
      // okay show results for amended Report options (removed version overrides)
     log.info( 'After: ' + mailOptions )

     // Now add in the version overrides and return final result
     mailOptions = mailOptions.concat( versionOptions );
     cb( null, mailOptions );     

    }   
  );
}


// Any Email option that has been overridden (by version) should be removed from report email options
function processVersionOverrides( reportOptions, versionOptions, mailOptions, versionOption, cb ) {

  // Iterate over Report email options and remove any matching the currentversion override option Type e.g. EMAIL_TO
  async.each(
    reportOptions,
    async.apply( removeOverrideOption, reportOptions, versionOptions, mailOptions, versionOption ),
    function ( err ) {
      if ( err ) {
        log.error( 'processVersionOverrides encountered error' );
        log.error( err );
        return;
      }
    }   
  );

  return cb( null );
}


// Check current Report options array element and remove it if it matches current Version option Type
function removeOverrideOption( reportOptions, versionOptions, mailOptions, versionOption, reportOption, cb ) {

  var vType,
    rType,
    index = 0;

  vType = versionOption[ 0 ];
  rType = reportOption[ 0 ];


  // If Report Email option Type matches the version override Type we are currently considering then remove it
  if ( vType === rType ) {

    log.debug( 'Match remove it: ' + vType + ' : ' + rType );

    index = mailOptions.indexOf( reportOption );
    if ( index > -1 ) { 

      mailOptions.splice( index, 1 );

    }
    
  } else { 

    log.debug( 'No match leave it: ' + vType + ' : ' + rType );

  }

  return cb( null );
}




// Query the Jde Email Configuration Setup for this Report / Version.
function queryJdeEmailConfig( connection, jdeJob, reportName, reportVersion, cb, emailConfig ) {

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
    
    processResultsFromF559890( connection, jdeJob, rs.resultSet, 1, cb, emailConfig );     

  });
}


// Process results of query on F559890 Jde Email Config Setup
function processResultsFromF559890( connection, jdeJob, rsF559890, numRows, cb, emailConfig ) {

  rsF559890.getRows( numRows, function( err, rows ) {
    if ( err ) {
      oracleResultSetClose( connection, rsF559890 );
      log.verbose( 'No email configuration found' );

      // Error so let caller know...
      cb( err, emailConfig, jdeJob );

    } else if ( rows.length == 0 ) {
      
      oracleResultSetClose( connection, rsF559890 );
      log.debug( 'Finished processing email configuration entries' );

      // Done processing so pass control to next function with results
      cb( null, emailConfig, jdeJob );

    } else if ( rows.length > 0 ) {
 
      emailConfigRecord = rows[ 0 ];
      log.debug( 'Email Record: ' + emailConfigRecord );

      // Process the Email Configuration record entry
      processEmailConfigEntry(  connection, emailConfigRecord, emailConfig );

      // Fetch next Email config entry
      processResultsFromF559890( connection, jdeJob, rsF559890, 1, cb, emailConfig );     
      
    }
  });
}


// Process each Email Configuration entry
function processEmailConfigEntry( connection, emailConfigRecord, emailConfig ) {

  emailConfig.push( [emailConfigRecord[ 3 ].trim(), emailConfigRecord[ 5 ].trim() ] );

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
