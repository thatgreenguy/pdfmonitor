var emailconfig = require( './common/emailconfig.js' ),
  log = require( './common/logger.js' ),
  dbCn = null,
  emailConfig = null,
  emailConfigOverrides = null,
  cb = null;




// Fetch email configuration for Report
emailconfig.fetchMailDefaults( dbCn, 'R41411', '', processReportMailConfig );




function processReportMailConfig( err, mailConfig ) {

  if ( err ) {
    log.error( 'Problem fetching report mail config' );
    return;
  } else {
    emailConfig = mailConfig;
    mailConfig = []; 
    log.info( 'Report Mail Config ------------------' );
    log.info( emailConfig );
    log.info( '-------------------------------------' );

    // Now get version overrides
    emailconfig.fetchMailDefaults( dbCn, 'R41411', 'UKX0002', processVersionMailConfig );


  }
}


function processVersionMailConfig( err, mailConfig ) {

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

  emailconfig.mergeMailOptions( emailConfig, emailConfigOverrides, printResult );

}

function printResult( err, emailOptions ) {

  log.info( 'Result: ' + emailOptions );

}
