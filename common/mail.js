// Module mail.js
//
// Establish SMTP transport
// Provide function to send Jde Ube report (PDF, CSV or both) via emal to configured receipients


var nodemailer = require( 'nodemailer' ),
  async = require( 'async' ),
  log = require( './logger.js' ),
  smtpTransport;


// Initialisation
// create re-usable transporter object using SMTP transport
smtpTransport = nodemailer.createTransport( "SMTP", {
  host: '172.31.3.15',
  port: 25
});


// Read email options from JDE database for this reprot then send the email
module.exports.prepMail = function( dbCn, jdeJob, postMailCb ) {

  var reportOptions = [],
    versionOptions = [];

  // Fetch default report mail options
  fetchMailDefaults( dbCn, jdeJob, postMailCb, false, reportOptions, versionOptions );

}


// Read Version override options from JDE database then merge the two
function getVersionOptions( dbCn, jdeJob, postMailCb, reportOptions, versionOptions ) {

  // Fetch report version override mail options
  fetchMailDefaults( dbCn, jdeJob, postMailCb, true, reportOptions, versionOptions );

}


// Merge both the Default options and any Version overrides to give final mail options 
function mergeAllOptions( dbCn, jdeJob, postMailCb, reportOptions, versionOptions ) {

  mergeMailOptions( jdeJob, reportOptions, versionOptions, postMailCb );

}


function cleanup() {

  // When finished with transport object do following....
  smtpTransport.close();

}


// Send Email
function sendMail( jdeJob, mailOptions, postMailCb ) {

  log.verbose( 'About to send email with these options : ' + mailOptions );
  
  // mailOptions holds mail configuration for this report from the JDE database
  // this could be as simple as a TO address as sensible defaults should be provided
  // by this program for any missing config.
  // In order to send an email the minimum required is an EMAIL = 'Y' and a TO address
  // defaults values will be supplied for rest

  var email,
    from = 'noreply@dlink.com',
    to = '',
    subject = '',
    cc = '', 
    bcc = '',
    text = '',
    attachments = [],
    wrk = {},
    entry,
    mo = {};

  from = 'noreply@dlink.com';
  subject = 'Dlink JDE Report : ' + jdeJob;
  text = 'Please find your JDE report attached to this email.'; 

  
  
  for ( var i = 0; i < mailOptions.length; i++ ) {

    entry = mailOptions[ i ];
    log.warn( entry );

    if ( entry[ 0 ] === 'EMAIL' ) {
      email = entry[ 1 ];
    }
    if ( entry[ 0 ] === 'EMAIL_TO' ) {
      if ( to ) {
        to += ', ' + entry[ 1 ];
      } else {
        to = entry[ 1 ];
      }
    }
    if ( entry[ 0 ] === 'EMAIL_CC' ) {
      if ( cc ) {
        cc += ', ' + entry[ 1 ];
      } else {
        cc = entry[ 1 ];
      }
    }
    if ( entry[ 0 ] === 'EMAIL_BCC' ) {
      if ( bcc ) {
        bcc += ', ' + entry[ 1 ];
      } else {
        bcc = entry[ 1 ];
      }
    }
    if ( entry[ 0 ] === 'EMAIL_SUBJECT' ) {
      subject = entry[ 1 ];
    }
    if ( entry[ 0 ] === 'EMAIL_TEXT' ) {
      text = entry[ 1 ];
    }
    if ( entry[ 0 ] === 'EMAIL_FROM' ) {
      from = entry[ 1 ];
    }

  }

  wrk[ 'filename' ] = jdeJob;
  wrk[ 'filePath' ] = '/home/pdfprint' + jdeJob;
  attachments[ 0 ] = wrk;

  if ( ! to ) {
 
    log.error( 'No TO recipient defined - unable to send this email' );
    email = 'N';

  }

  log.verbose( 'EMAIL: ' + email );
  log.verbose( 'FROM: ' + from );
  log.verbose( 'TO: ' + to );
  log.verbose( 'SUBJECT: ' + subject );
  log.verbose( 'CC: ' + cc );
  log.verbose( 'BCC: ' + bcc );
  log.verbose( 'TEXT: ' + text );
  log.verbose( 'ATT: ' + attachments );

  mo['from'] = from;
  mo['to'] = to;
  mo['subject'] = subject;
  mo['text'] = text;
  if ( cc ) {
    mo['cc'] = from;
  }
  if ( bcc ) {
    mo['bcc'] = from;
  }
  mo['attachments'] = attachments;

  if ( email === 'Y' ) {
    smtpTransport.sendMail( mo, 
    function(err, response) {
  
      if ( err ) {
        log.error( 'Error trying to send email - failed' );
        log.error( err );
       return postMailCb( err );

      } else {

        log.verbose( "Email sent: " + response.message);
        postMailCb( null, 'Mail Sent');
      }
    });
  } 
}


// Fetch default email configuration for given Jde report name.
function fetchMailDefaults( dbCn, jdeJob, postMailCb, useVersion, reportOptions, versionOptions ) {

  var reportName,
    versionName,
    tokens;

  // Extract Report name and Version name
  tokens = jdeJob.split('_');
  reportName = tokens[ 0 ];

  // For default Report options version name is '*ALL' - otherwise pass actual version name
  if ( useVersion ) {

    versionName = tokens[ 1 ];

  } else {

    versionName = '*ALL';

  }

  queryJdeEmailConfig( dbCn, jdeJob, postMailCb, reportName, versionName, reportOptions, versionOptions );

}


// Query the Jde Email Configuration Setup for this Report / Version.
function queryJdeEmailConfig( dbCn, jdeJob, postMailCb, reportName, versionName, reportOptions, versionOptions ) {

  var query;

  log.debug( 'Fetch email config for Report: ' + reportName + ' version: ' + versionName );

  query = "SELECT * FROM testdta.F559890 WHERE CRPGM = '" + reportName;
  query += "' AND CRVERNM = '" + versionName + "'";
  query += " AND CRCFGSID = 'PDFMAILER'";
  
  log.debug( query ); 

  dbCn.execute( query, [], { resultSet: true }, 
  function( err, rs ) {
  
    if ( err ) {
    
      log.error( 'Query Failed : queryJdeEmailConfig Failed' );
      log.error( err.message );
      return;

    }
    
    processResultsFromF559890( dbCn, jdeJob, postMailCb, versionName, rs.resultSet, 1, reportOptions, versionOptions );     

  });
}


// Process results of query on F559890 Jde Email Config Setup
function processResultsFromF559890( dbCn, jdeJob, postMailCb, versionName, rsF559890, numRows, reportOptions, versionOptions ) {

  rsF559890.getRows( numRows, 
  function( err, rows ) {
    if ( err ) {

      oracleResultSetClose( dbCn, rsF559890 );
      log.verbose( 'No email configuration found' );
      
    } else if ( rows.length == 0 ) {
      
      oracleResultSetClose( dbCn, rsF559890 );
      log.debug( 'Finished processing email configuration entries' );

      // Done processing so pass control to next function with results
      if ( versionName === '*ALL' ) {
        getVersionOptions( dbCn, jdeJob, postMailCb, reportOptions, versionOptions );
      } else {
        mergeAllOptions( dbCn, jdeJob, postMailCb, reportOptions, versionOptions );
      }
      
    } else if ( rows.length > 0 ) {
 
      log.debug( 'Email Record: ' + rows[ 0 ] );

      // Process the Email Configuration record entry
      processEmailConfigEntry(  dbCn, jdeJob, postMailCb, rsF559890,
        rows[ 0 ], versionName, reportOptions, versionOptions );

    }
  });
}


// Process each Email Configuration entry
function processEmailConfigEntry( dbCn, jdeJob, postMailCb, rsF559890, record,
  versionName, reportOptions, versionOptions ) {

  log.warn( 'Rptoptions: ' + reportOptions );
  log.warn( 'Veroptions: ' + versionOptions );


  if ( versionName === '*ALL' ) {
    reportOptions.push([ record[ 3 ].trim(), record[ 5 ].trim() ]);
  } else {
    versionOptions.push([ record[ 3 ].trim(), record[ 5 ].trim() ]);
  }

  // Fetch next Email config entry
  processResultsFromF559890( dbCn, jdeJob, postMailCb, versionName, rsF559890, 1, reportOptions, versionOptions );     
    
}


// Multiple email options can be defined for a Report and any of those options can be overridden at report/version level.
// This function takes the options for the report and those for the version overrides (if any) and returns a merged set of 
// options.
// if same option is defined at version level as at report level then the report level option is completely 
// replaced by the version override.
// Otherwise the result is a combination of report and version specific options.
function mergeMailOptions( jdeJob, reportOptions, versionOptions, postMailCb ) {

  log.warn( 'reportoptions: ' + reportOptions );
  log.warn( 'versionoptions: ' + versionOptions );


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
        return postMailCb( err );     
      }    
  
      // okay show results for amended Report options (removed version overrides)
     log.info( 'After: ' + mailOptions )

     // Now add in the version overrides and return final result
     mailOptions = mailOptions.concat( versionOptions );
     sendMail( jdeJob, mailOptions, postMailCb );     

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
function oracleDbConnectionRelease( connection ) {

  connection.release( function( err ) {
    if ( err ) {
      log.error( 'Failed to release/cleanup DB connection' );
      log.error( err );
      return;
    }
  });
}






// Build options for mail sending using passed JDE Report / Job Name
module.exports.testMail = function( dbCn, jdeJob, cb ) {

  // Test Email 
  var mailOptions = {
    from: "noreply@dlink.com",
    to: "paul.green@dlink.com, thatgreenguy@gmx.co.uk",
    subject: "Hi - this is a test email from Node on Centos",
    text: "Hello - Testing Testing 1 2 3 ...",
    html: "<P>Hello - Testing Testing 1 2 3 ...",
    attachments: [{ fileName: 'R5542565', filePath: "/home/pdfdata/R5542565_ESXCOC02_183614_PDF" }]
    };

  // Fetch mail configuration for passed Jde Job here and send the email
  // Once done pass control on to given callback
  module.exports.sendMail( mailOptions, cb );

}


