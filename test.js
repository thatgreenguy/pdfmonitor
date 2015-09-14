var emailconfig = require( './common/emailconfig.js' ),
  log = require( './common/logger.js' ),
  dbCn = null,
  ecList = null;


log.debug( 'Start Test ' );
ecList = emailconfig.fetchMailDefaults( dbCn, 'R5542565', '');
log.debug( 'Done  Test ' );

