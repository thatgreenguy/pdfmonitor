// Module		: audit.js
// Description		: Common Audit file logging related functions.
// Author		: Paul Green
// Dated		: 2015-08-03
//
// Application maintains a simple audit log file within Jde which is used for informational purposes and by the
// application itself to determine the last Pdf processed - determines date and time to run query checks from. 
  

var oracledb = require( 'oracledb' ),
  logger = require( './logger' ),
  moment = require( 'moment' ),
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME };


// Insert new Audit entry into the JDE audit log file.
exports.createAuditEntry = function( pdfjob, genkey, ctrid, status ) {

  var dt,
  timestamp,
  jdedate,
  jdetime,
  query;

  dt = new Date();
  timestamp = exports.createTimestamp( dt );
  jdedate = exports.getJdeJulianDate( dt );
  jdetime = exports.getJdeAuditTime( dt );

  if ( typeof( pdfjob ) === 'undefined' ) pdfjob = ' ';
  if ( typeof( genkey ) === 'undefined' ) genkey = ' ';
  if ( typeof( ctrid ) === 'undefined' ) ctrid = ' ';
  if ( typeof( status ) === 'undefined' ) status = ' ';

  oracledb.getConnection( credentials, function(err, connection) {
    if ( err ) { 
      logger.error( 'Oracle DB Connection Failure' );
      return;
    }

    query = "INSERT INTO testdta.F559859 VALUES (:pasawlatm, :pafndfuf2, :pablkk, :paactivid, :padeltastat, :papid, :pajobn, :pauser, :paupmj, :paupmt)";
 
    connection.execute( query, [timestamp, pdfjob, genkey, ctrid, status, 'PDFHANDLER', 'CENTOS', 'DOCKER', jdedate, jdetime ], { autoCommit: true }, function( err, result ) {
      if ( err ) {
        logger.error( err.message );
        return;
      }
      connection.release( function( err ) {
        if ( err ) {
          logger.error( err.message );
          return;
        }
      });
    });
  });
}


// Create human readable timestamp string suitable for Audit Logging - Returns timestamp string like 'YYYY-MM-DD T HH:MM:SS MMMMMMMMM'
// Date and time elements are padded with leading '0' by default. Date and Time separator characters are '-' and ':' by default.
// MMMMMMMMM is time as milliseconds since epoch to keep generated string unique for same second inserts to Audit Log table. 
exports.createTimestamp = function( dt, dateSep, timeSep, padChar ) {

  if ( typeof( dt ) === 'undefined' ) dt = new Date();
  if ( typeof( dateSep ) === 'undefined' ) dateSep = '-';
  if ( typeof( timeSep ) === 'undefined' ) timeSep = ':';
  if ( typeof( padChar ) === 'undefined' ) padChar = '0';
  
  return dt.getFullYear() + dateSep + ( padChar + ( dt.getMonth() + 1 ) ).slice( -2 ) + dateSep + ( padChar + dt.getDate() ).slice( -2 )
    + ' T ' + ( padChar + dt.getHours() ).slice( -2 ) + timeSep + ( padChar + dt.getMinutes() ).slice( -2 ) + timeSep
    + ( padChar + dt.getSeconds() ).slice( -2 ) + ' ' + dt.getTime();
}


// Converts date to JDE Julian style date i.e. CYYDDD
exports.getJdeJulianDate = function( dt ) {

  var wkM,
    wkYYYY,
    wkDDD;

  if ( typeof( dt ) === 'undefined' ) dt = new Date();

  wkM = moment( dt );

  wkYYYY = wkM.year();
  wkDDD = wkM.dayOfYear();
  return wkYYYY - 1900 + ( '000' + wkDDD).slice( -3 );

}


// Convert date to JDE Audit Time HHMMSS - Return jde Audit time in format HHMMSS with no separators and leading 0's if required.
exports.getJdeAuditTime = function( dt, padChar ) {

  var jdetime;

  if ( typeof( dt ) === 'undefined' ) dt = new Date();
  if ( typeof( padChar ) === 'undefined' ) padChar = '0';

  jdetime = ( padChar + dt.getHours() ).slice( -2 ) + ( padChar + dt.getMinutes() ).slice( -2 ) + ( padChar + dt.getSeconds() ).slice( -2 );

  return jdetime;
}


// Reduce audit timestamp value by x minutes and return adjusted value plus Jde date and Time equivalents
// Accepts timestamp string (from audit file) and returns date adjusted by x minutes
// as well as JDE Julian Date and JDE Julian Time Audit value equivalants 
exports.adjustTimestampByMinutes = function( timestamp, mins ) {

  var millisecs = null,
  n = null,
  dt = new Date(),
  adjdt = null,
  newdt,
  newtm;
	
  // Date and Time should be passed if not set to zeros and return adjusted by minutes value
  if ( typeof( mins ) === 'undefined' ) mins = -5;
  if ( typeof( timestamp ) !== 'undefined' ) {
    millisecs = timestamp.substr( 22, 13 );
    n = parseInt( millisecs );
    dt = new Date( n );
  }

  // Get timestamp date adjusted by however minutes
  adjdt = new Date( dt.setMinutes( dt.getMinutes() + mins ) );
	
  // Return Jde Julian style Date and Times 
  newdt = module.exports.getJdeJulianDate( adjdt );
  newtm = module.exports.getJdeAuditTime( adjdt );

  return {'jdeDate': newdt, 'jdeTime': newtm, 'timestamp': dt, 'minutes': mins };
}


// This proces usually monitors JDE Report queue from the last Date and Time it handled a JDE report
// Reference the Mailer audit log to get Date and Time of the Last emailed report
// If Audit Log file has benn cleared or this is genuinely the first time the pdfmailer has run then 
// use current Date and Time as the point to start monitoring from.
// Running from the date and time of the last emailed report allows for recovery on startup should this process
// or its host server be taken off line for a some reason - on restart it will recover and email everything it should have done!
exports.determineLastProcessedDateTime = function( err, dbCn, cb ) {

  var query = null;
	
  query  = "SELECT paupmj, paupmt, pasawlatm, pafndfuf2, pablkk FROM testdta.F559849 ";
  query += "WHERE RTRIM(PAFNDFUF2, ' ') <> 'PDFMAILER' ORDER BY pasawlatm DESC";

  dbCn.execute( query, [], { resultSet: true }, function( err, rs ) {
    if ( err ) {
      log.error( err.message )
      return cb( err );
    };

    processResultsFromF559849( dbCn, rs.resultSet, cb );

  });
}



// Process results from JDE Audit Log table Query but only interested in last Pdf job processed
// to determine date and time which is required to begin monitoring JDE report queue
function processResultsFromF559849( err, dbCn, rsF559849, numRows, cb) {

  var auditRecord;

  rsF559849.getRows( numRows, function( err, rows ) {
    if ( err ) {

      oracleResultsetClose( dbCn, rsF559849 );
      return cb( err );

    } else if ( rows.length == 0 ) {

      queryJdeJobControl( dbCn, null, begin, pollInterval, hostname, lastPdf, performPolledProcess );
      oracleResultsetClose( dbCn, rsF559849 );

    } else if ( rows.length > 0 ) {

      // Last audit entry retrieved
      // Process continues by querying the JDE Job Control Master file for eligible PDF's to process

      record = rows[ 0 ];
      queryJdeJobControl( dbCn, record, begin, pollInterval, hostname, lastPdf, performPolledProcess );
      oracleResultsetClose( dbCn, rsF559849 );
    }
  });
}


// Close Oracle database result set
function oracleResultsetClose( dbCn, rs ) {

  rs.close( function( err ) {
    if ( err ) {
      log.error( err );
      oracledbCnRelease( dbCn );
    }
  });
}


// Close Oracle database Connection
function oracledbCnRelease( dbCn ) {

  dbCn.release( function ( err ) {
    if ( err ) {
      log.error( err );
    }
  });
}
