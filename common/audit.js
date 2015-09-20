// Module		: audit.js
// Description		: Common Audit file logging related functions.
// Author		: Paul Green
// Dated		: 2015-08-03
//
// Application maintains a simple audit log file within Jde which is used for informational purposes and by the
// application itself to determine the last Pdf processed - determines date and time to run query checks from. 
  

var oracledb = require( 'oracledb' ),
  log = require( './logger' ),
  moment = require( 'moment' ),
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  aixTimeOffset = process.env.AIX_TIME_OFFSET;


// Initialisation
//
// AIX Time offset required because all servers synchronised except AIX which is currently adrift by approx 3.5 minutes
// So date and Time related database queries could miss jobs created on AIX unless we offset the time
if ( typeof( aixTimeOffset ) === 'undefined' ) {

  aixTimeOffset = -115;
  log.debug( 'AIX Server Time Offset will be : ' + aixTimeOffset + ' for this run.' );

}

exports.aixTimeOffset = aixTimeOffset;


// Functions - 
//
// exports createAuditEntry function( odbCn, pdfjob, genkey, ctrid, status, dbCn )
// exports createTimestamp function( dt, dateSep, timeSep, padChar ) 
// exports getJdeJulianDate function( dt ) 
// exports getJdeAuditTime function( dt, padChar ) 
// exports adjustTimestampByMinutes function( timestamp, mins ) 
// exports determineLastProcessedDateTime function( err, dbCn, cb ) 
// processResultsFromF559849( dbCn, rsF559849, numRows, cb) 
// oracleResultSetClose( dbCn, rs ) 
// oracledbCnRelease( dbCn ) 


// Insert new Audit entry into the JDE audit log file.
exports.createAuditEntry = function( odbCn, pdfjob, genkey, ctrid, status, dbCn ) {

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

  query = "INSERT INTO testdta.F559849 VALUES (:pasawlatm, :pafndfuf2, :pablkk, :paactivid, :padeltastat, :papid, :pajobn, :pauser, :paupmj, :paupmt)";
  log.debug( query );

  odbCn.execute( query, 
    [timestamp, pdfjob, genkey, ctrid, status, 'PDFMAILER', 'CENTOS', 'DOCKER', jdedate, jdetime ],
    { autoCommit: true }, 
    function( err, rs ) {

      if ( err ) {
        log.error( err.message );
        return;
      }
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
  if ( typeof( mins ) === 'undefined' ) mins = aixTimeOffset;
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
  query += "WHERE RTRIM(PAFNDFUF2, ' ') <> 'pdfmailer' ORDER BY pasawlatm DESC";

  dbCn.execute( query, [], { resultSet: true }, 
  function( err, rs ) {

    if ( err ) {
      log.error( err.message )
      return cb( err, null );
    };

    processResultsFromF559849( dbCn, rs.resultSet, 1, cb );

  });
}



// Process results from JDE Audit Log table Query but only interested in last Pdf job processed
// to determine date and time which is required to begin monitoring JDE report queue
function processResultsFromF559849( dbCn, rsF559849, numRows, cb) {

  var auditRecord,
    tokens,
    data = {},
    ts = null;
    ats = null;

  rsF559849.getRows( numRows, function( err, rows ) {
    if ( err ) {

      oracleResultSetClose( dbCn, rsF559849 );
      cb( err, null );

    } else if ( rows.length == 0 ) {

      log.verbose( 'Last Audit Entry: Not found use current Date and Time' );
      oracleResultSetClose( dbCn, rsF559849 );

      // Get current Date and Time adjusted by Aix Server Time Offset
      ts = exports.createTimestamp();      
      ats = exports.adjustTimestampByMinutes( ts );  

      data[ 'lastAuditEntryDate' ] = ats.jdeDate;
      data[ 'lastAuditEntryTime' ] = ats.jdeTime;
      data[ 'lastAuditEntryJob' ] = 'None';

      cb( null, data );

    } else if ( rows.length > 0 ) {

      // Last audit entry retrieved
      // Determine Date and Time to start monitoring from then pass control onwards
      log.verbose( 'Last Audit Entry: ' + rows[ 0 ] );
      oracleResultSetClose( dbCn, rsF559849 );

      jdedatetime = rows[ 0 ][ 4 ];
      tokens = jdedatetime.split(' ');

      data[ 'lastAuditEntryDate' ] = tokens[ 0 ];
      data[ 'lastAuditEntryTime' ] = tokens[ 1 ];
      data[ 'lastAuditEntryJob' ] = rows[ 0 ][ 3 ];

      cb( null, { 'lastAuditEntryDate': tokens[ 0 ], 
                  'lastAuditEntryTime': tokens[ 1 ],
                  'lastAuditEntryJob': rows[ 0 ][ 3 ]} );
    }
  });
}


// Close Oracle database result set
function oracleResultSetClose( dbCn, rs ) {

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


