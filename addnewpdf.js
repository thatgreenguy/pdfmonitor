var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  log = require( './common/logger.js' ),
  audit = require( './common/audit.js' ),
  hostname = process.env.HOSTNAME,
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };
  

module.exports.addNewPdf = function( pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = { autoCommit:true },
    newPdf;

  
  newPdf = pargs.newPdfRow[ 0 ];
  log.v( newPdf + ' : Get Connection for Insert' );

  oracledb.getConnection( credentials, function( err, dbc ) {

    if ( err ) {
      log.e( newPdf + ' : Unable to get Jde DB connection : ' + err );     
      return cbWhenDone( err );
    }  

    sql = " INSERT INTO " + jdeEnvDb.trim() + ".F559811 VALUES (:jpfndfuf2, :jpsawlatm, :jpactivid, :jpyexpst, :jpblkk, :jppid, :jpjobn, :jpuser, :jpupmj, :jpupmt ) ";
    binds.push( pargs.newPdfRow[ 0 ] );
    binds.push( audit.createTimestamp() );
    binds.push( hostname );
    binds.push( '100' );
    binds.push( pargs.newPdfRow[ 1 ] + ' ' + pargs.newPdfRow[ 2 ] );
    binds.push( 'PDFMONITOR' );
    binds.push( 'CENTOS' );
    binds.push( 'DOCKER' );
    binds.push( pargs.newPdfRow[ 1 ] );
    binds.push( pargs.newPdfRow[ 2 ] );

    dbc.execute( sql, binds, options, function( err, result ) {

      if ( err ) {
        log.e( newPdf + ' : Jde Db Query insert failed : ' + err );
        dbc.release( function( err ) {
          if ( err ) {
            log.e( newPdf + ' : Unable to release Jde Db connection : ' + err );
            return cbWhenDone( err );
          }
        });     
        return cbWhenDone( err );
      }  

      log.v( newPdf + ' : Inserted  following row from F559811 : ' + pargs.newPdfRow );
      dbc.release( function( err ) {
        if ( err ) {
          log.e( newPdf + ' : Unable to release Jde Db connection : ' + err );
          return cbWhenDone( err );
        }
        return cbWhenDone( null, newPdf + ' Inserted' );
      });          
    });
  });

}
