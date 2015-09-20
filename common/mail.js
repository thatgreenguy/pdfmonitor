// Module		: mail.js
// Description		: Common PDF file mail related functions.
// Author		: Paul Green
// Dated		: 2015-09-20
//
// Accepts a pdf file name e.g. 'R5542565_ESXCOC01_123456' 
// Extracts Report name and Version information
// Checks JDE mail configuration for report and version
// Performs configured mail functionality for report/version 

  
var oracledb = require('oracledb'),
  emailconfig = require( './emailconfig.js' ),
  audit = require( './audit.js' ),
  log = require( './logger.js' ),
  nodemailer = require('nodemailer'),
  emailConfig = null,
  emailConfigOverrides = null,
  report,
  version, 
  smtpTransport;


// create re-usable transporter object using SMTP transport
smtpTransport = nodemailer.createTransport( "SMTP", { host: '172.31.3.15', port: 25 } );


// Functions -
//
// module.exports.mailMe( odbCn, jdeJob )
// function processReportMailConfig( err, mailConfig, jdeJob )
// function processVersionMailConfig( err, mailConfig, jdeJob )
// function mergeReportAndVersionOptions()
// function handleEmailing( err, emailOptions )


// Call this function with a JDE job name and any configured mail requirements will be done.
module.exports.mailMe = function( odbCn, jdeJob ) {

  var tokens;

  // Extract Report Name from Jde Job Namne
  tokens = jdeJob.split('_');
  report = tokens[ 0 ];

  log.debug( 'Mail request received for: ' + jdeJob + ' Report: ' + report + ' Version: ' + version );

  // Fetch email configuration for Report
  emailconfig.fetchMailDefaults( odbCn, jdeJob, report, '', processReportMailConfig );


}


function processReportMailConfig( err, mailConfig, jdeJob ) {

  var tokens;

  if ( err ) {

    log.error( 'Problem fetching report mail config' );
    return;

  } else {

    emailConfig = mailConfig;
    mailConfig = []; 
    log.info( 'Report Mail Config ------------------' );
    log.info( emailConfig );
    log.info( '-------------------------------------' );

    // Extract Version Name
    tokens = jdeJob.split('_');
    version = tokens[ 1 ];

    // Now get version overrides
    emailconfig.fetchMailDefaults( dbCn, jdeJob, report, version, processVersionMailConfig );


  }
}


function processVersionMailConfig( err, mailConfig, jdeJob ) {

  if ( err ) {

    log.error( 'Problem fetching report version overrides mail config' );
    return;

  } else {

    emailConfigOverrides = mailConfig; 
    log.info( 'Report Version Overrides Mail Config -' );
    log.info( emailConfigOverrides );
    log.info( '--------------------------------------' );

  }

  mergeReportAndVersionOptions();

}


function mergeReportAndVersionOptions() {

  log.debug( 'Report options: ' + emailConfig );
  log.debug( 'Version options: ' + emailConfigOverrides );

  emailconfig.mergeMailOptions( emailConfig, emailConfigOverrides, handleEmailing );

}


function handleEmailing( err, emailOptions ) {

  log.info( 'Actual Email Options : ' + emailOptions );

  // Set-up Email 
  var mailOptions = {
    from: "no.reply@dlink.com",
    to: "paul.green@dlink.com",
    subject: "Hi - this is a test email from Node on Centos",
    text: "Hello - Testing Testing 1 2 3 ...",
    html: "<P>Hello - Testing Testing 1 2 3 ..."
  } 

  // Send Email
  smtpTransport.sendMail( mailOptions, 
  function(error, response) {
  
  if (error) {
    
    console.log(error);

  } else {

    console.log("Message sent: " + response.message);

  }

  // When finished with transport object do following....
  smtpTransport.close();

  });
}





