var async = require( 'async' ),
  oracledb = require( 'oracledb' ),
  moment = require( 'moment' ),
  log = require( './common/logger.js' ),
  audit = require( './common/audit.js' ),
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB,
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110,
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  timeOffset = 0;
  

module.exports.getNewPdf = function(  pargs, cbWhenDone ) {

  var p = {},
    sql,
    binds = [],
    options = {},
    row;

  log.v( 'Get Connection to query for any new PDF entries since last PDF Job added to Queue' );

  oracledb.getConnection( credentials, function( err, dbc ) {

    if ( err ) {
      log.e( ' Unable to get Jde DB connection : ' + err );     
      return cbWhenDone( err );
    }  

    sql = constructQuery( '115300', '033000', timeOffset );
    dbc.execute( sql, binds, options, function( err, result ) {

      if ( err ) {
        log.e( ' Jde Db Query execution failed : ' + err );
        dbc.release( function( err ) {
          if ( err ) {
            log.e( ' Unable to release Jde Db connection : ' + err );
            return cbWhenDone( err );
          }
        });     
        return cbWhenDone( err );
      }  

      pargs.newPdfRows = result.rows;
      log.v( 'Read following rows from Jde Job Control : ' + result );
      dbc.release( function( err ) {
        if ( err ) {
          log.e( ' Unable to release Jde Db connection : ' + err );
          return cbWhenDone( err );
        }
        return cbWhenDone( null, row );
      });          
    });
  });

}



// Construct query which is suitable for monitor from date and time and considering
// change of day on both startup and crossing midnight boundary
function constructQuery( monitorFromDate, monitorFromTime, timeOffset ) {

  var query = null,
      currentMomentAix,
      jdeTodayAix,
      jdeEnvCheck;

  // Process expects a JDE environment to be specified via environment variables so post PDF processing can be 
  // isolated for each JDE environment DV, PY, UAT and PROD
  // therefore environment check needs to be part of query restrictions so construct that here
  if ( jdeEnv === 'DV812' ) {
    jdeEnvCheck = " AND (( jcenhv = 'DV812') OR (jcenhv = 'JDV812')) "; 
  } else {
    if ( jdeEnv === 'PY812' ) {
      jdeEnvCheck = " AND (( jcenhv = 'PY812') OR (jcenhv = 'JPY812') OR (jcenhv = 'UAT812') OR (jcenhv = 'JUAT812')) "; 
    } else {
      if ( jdeEnv === 'PD812' ) {
        jdeEnvCheck = " AND (( jcenhv = 'PD812') OR ( jcenhv = 'JPD812')) ";      
      }
    }
  }



  // We have monitorFromDate to build the JDE Job Control checking query, however, we need to also account for 
  // application startups that are potentially checking from a few days ago plus we need to account for when we are 
  // repeatedly monitoring (normal running mode) and we cross the midnight threshold and experience a Date change

  // Check the passed Monitor From Date to see if it is TODAY or not - use AIX Time not CENTOS
  currentMomentAix = moment().subtract( timeOffset); 
  jdeTodayAix = audit.getJdeJulianDateFromMoment( currentMomentAix );

  log.d( 'Check Date is : ' + monitorFromDate + ' Current (AIX) JDE Date is ' + jdeTodayAix );

  if ( monitorFromDate == jdeTodayAix ) {

    // On startup where startup is Today or whilst monitoring and no Date change yet
    // simply look for Job Control entries greater than or equal to monitorFromDate and monitorFromTime
     
    query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM " + jdeEnvDbF556110.trim() + ".F556110 ";
    query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' " + jdeEnvCheck;
    query += " AND jcactdate = " + monitorFromDate + ' AND jcacttime >= ' + monitorFromTime;
    query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
    query += " ( SELECT RTRIM(crpgm, ' ') FROM " + jdeEnvDb.trim() + ".F559890 WHERE crcfgsid = 'PDFMAIL' OR crcfgsid = 'PDFLOGO' )";
    query += " ORDER BY jcactdate, jcacttime";  

  } else {

    // Otherwise Startup was before Today or we have crossed Midnight into a new day so query needs to adjust
    // and check for records on both sides of the date change

    query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM " + jdeEnvDbF556110.trim() + ".F556110 ";
    query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' " + jdeEnvCheck;
    query += " AND (( jcactdate = " + monitorFromDate + " AND jcacttime >= " + monitorFromTime + ") ";
    query += " OR ( jcactdate > " + monitorFromDate + " )) ";
    query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
    query += " ( SELECT RTRIM(crpgm, ' ') FROM " + jdeEnvDb.trim() + ".F559890 WHERE crcfgsid = 'PDFMAIL' OR crcfgsid = 'PDFLOGO' ) ";
    query += " ORDER BY jcactdate, jcacttime";
  
  }

  log.d( query );

  return query;

}
// pdfprocessqueue : Maintains entries in the custom JDE table F559811 DLINK Post PDF Handling Queue
// Author          : Paul Green
// Dated           : 2015-10-03
//
// Synopsis
// --------
//
// Use this module to check the last entry made to the queue or add a new entry to the queue

var log = require( './common/logger.js' ),
  odb = require( './common/odb.js' ),
  audit = require( './common/audit.js' ),
  jdeDB = process.env.JDEDB,
  hostname = process.env.HOSTNAME,
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB;


// Functions -
//
// module.exports.getLatestQueueEntry = function( pool, cb )
// module.exports.getPdfEntry = function( pool, pdf, cb )
// module.exports.getEnterpriseServerSystemDateTime = function( pool, cb )
// module.exports.addJobToProcessQueue = function( pool, dbc, row, cb )


// Grab a connection from the Pool, query the database for the latest Queue entry
// release the connection back to the pool and finally return to caller with date/time of last entry
module.exports.getLatestQueueEntry = function( pool, cb ) {

  var response = {},
    cn = null;

  response.error = null;
  response.result = null;

  
  // Ensure Oracle resources released before handing response back to caller
  function releaseReturn() {

    if ( cn ) {

      odb.releaseConnection( cn, 
      function( err, result ) {
 
        if ( err ) {

          log.e( 'Failed to release Oracle Connection back to Pool' + err );
          return cb( err );

        } else {

         log.d( ' Response : Error: ' + response.error + ' Result: ' + response.result );

         return cb( response.error, response.result ); 

        }
      });    
    }
  }  


  function closeResultSet( rs ) {

    odb.closeSelectSet( cn, rs, function( err ) {

      if ( err ) {

        log.e( 'FAILED to close result set' + err );
        releaseReturn();            

      } else {

        log.d( 'Result Set closed now release connection' );
        releaseReturn();            

      }
    });
  }


  function processConnection( err, connection ) {

    var query = null,
      binds = [],
      options = { resultSet: true };

    if ( err ) {

      log.e( 'Failed to get a connection' );
      log.e( err );

      response.error = err;
      releaseReturn();
      
    } else {

      // Make returned connection available to other functions
      cn = connection;
 
      query = "SELECT jpfndfuf2, jpblkk FROM " + jdeEnvDb.trim() + ".F559811 ORDER BY jpupmj DESC, jpupmt DESC";

      odb.performSQL( cn, query, binds, options, processResult );

    }
  }

  function processResult( err, rs ) {

    if ( err ) {

      log.e( 'Failed to get a Select result' );
      log.e( err );

      response.error = err;
      response.result = null;
      releaseReturn();

    } else {

      rs.resultSet.getRows( 1, function( err, rows ) {

        if ( err ) {

          log.e( 'Failed to get Row from resultset' );
          log.e( err );

          response.error = err;
          closeResultSet( rs );

        } else if ( rows.length == 0 ) {

          log.d( 'No records found in JDE process Queue table has been cleared / is empty' );

          response.result = null;
          closeResultSet( rs );

        } else if ( rows.length > 0 ) {

          log.d( 'We have latest processed entry from queue: ' + rows[ 0 ] ); 
  
          response.result = rows[ 0 ];
          closeResultSet( rs );

        }   
      });
    }
  }


  // Get a pooled connection, check the Jde PDF process Queue, cleanup and return
  odb.getConnection( pool, processConnection );

}


// Generally want to avoid duplicate key errors when inserting new PDF entries to Pdf Process Queue
// This function checks to see if a PDF already exists in the file or not
// If not returned result will be null otherwise it will contain the row
module.exports.getPdfEntry = function( pool, pdf, cb ) {

  var response = {},
    cn = null;

  response.error = null;
  response.result = null;

  
  // Ensure Oracle resources released before handing response back to caller
  function releaseReturn() {

    if ( cn ) {

      odb.releaseConnection( cn, 
      function( err, result ) {
 
        if ( err ) {

          log.e( 'Failed to release Oracle Connection back to Pool' + err );
          return cb( err );

        } else {

         log.d( ' Response : Error: ' + response.error + ' Result: ' + response.result );

         return cb( response.error, response.result ); 

        }
      });    
    }
  }  


  function closeResultSet( rs ) {

    odb.closeSelectSet( cn, rs, function( err ) {

      if ( err ) {

        log.e( 'FAILED to close result set' + err );
        releaseReturn();            

      } else {

        log.d( 'Result Set closed now release connection' );
        releaseReturn();            

      }
    });
  }


  function processConnection( err, connection ) {

    var query = null,
      binds = [],
      options = { resultSet: true };

    if ( err ) {

      log.e( 'Failed to get a connection' );
      log.e( err );

      response.error = err;
      releaseReturn();
      
    } else {

      // Make returned connection available to other functions
      cn = connection;
 
      query = "SELECT * FROM " + jdeEnvDb.trim() + ".F559811 WHERE jpfndfuf2 = '" + pdf.trim() + "' ";

      odb.performSQL( cn, query, binds, options, processResult );

    }
  }

  function processResult( err, rs ) {

    if ( err ) {

      log.e( 'Failed to get a Select result' );
      log.e( err );

      response.error = err;
      response.result = null;
      releaseReturn();

    } else {

      rs.resultSet.getRows( 1, function( err, rows ) {

        if ( err ) {

          log.e( 'Failed to get Row from resultset' );
          log.e( err );

          response.error = err;
          closeResultSet( rs );

        } else if ( rows.length == 0 ) {

          log.d( 'PDF : ' + pdf + ' Is not in the F559811 JDE PDF Process Queue' );

          response.result = null;
          closeResultSet( rs );

        } else if ( rows.length > 0 ) {

          log.d( 'PDF : ' + pdf + ' Is already in the F559811 JDE PDF Process Queue' );
  
          response.result = rows[ 0 ];
          closeResultSet( rs );

        }   
      });
    }
  }


  // Get a pooled connection, check the Jde PDF process Queue, cleanup and return
  odb.getConnection( pool, processConnection );

}


// Get the current System Time from the Oracle database host.
// Note: Unable to simply use this application host date and time because the AIX time is out of sync with all other servers
// Generally all servers are kept synchronised with NTP across DLINK but the JDE Enterprise server (AIX) is not and is 
// currently running approx 3 minutes SLOW! Have requested Infrastructure to address this issue preferably via install of NTP
// on AIX...
// Grab Oracle DB System Time in a format we can easily use, release the connection back to the pool then return to caller with result
module.exports.getEnterpriseServerSystemDateTime = function( pool, cb ) {

  var response = {},
    cn = null;

  response.error = null;
  response.result = null;

  
  // Ensure Oracle resources released before handing response back to caller
  function releaseReturn() {

    if ( cn ) {

      odb.releaseConnection( cn, 
      function( err, result ) {
 
        if ( err ) {

          log.e( 'Failed to release Oracle Connection back to Pool' );
          return cb( err );

        } else {

         log.d( ' Response Error: ' + response.error + ' Result: ' + response.result );
         return cb( response.error, response.result ); 

        }
      });    
    }
  }  


  function closeResultSet( rs ) {

    odb.closeSelectSet( cn, rs, function( err ) {

      if ( err ) {

        releaseReturn();            

      } else {

        releaseReturn();            

      }
    });
  }


  function processConnection( err, connection ) {

    var query = null,
      binds = [],
      options = { resultSet: true };

    if ( err ) {

      log.e( 'Failed to get a connection' );
      log.e( err );
      
      return cb( err );

    } else {

      // Make returned connection available to other functions
      cn = connection;

      query = 'SELECT TO_CHAR(SYSDATE, ';
      query += "'" + 'YYYY-MM-DD HH24:MI:SS' + "'" + ') "NOW" FROM DUAL ';

      odb.performSQL( cn, query, binds, options, processResult );

    }
  }

  function processResult( err, rs ) {

    if ( err ) {

      log.e( 'Failed to get a Select result' );
      log.e( err );

      response.error = err;
      response.result = null;
      releaseReturn();

    } else {

      rs.resultSet.getRows( 1, function( err, rows ) {

        if ( err ) {

          log.e( 'Failed to get Oracle Date and Time Row from resultset' );
          log.e( err );

          response.error = err;
          closeResultSet( rs );

        } else if ( rows.length == 0 ) {

          log.w( 'No records returned from SYSTEM Date and Time query to Oracle' );

          response.err = new Error( 'Failed to get System Date and Time from Oracle Host' );
          closeResultSet( rs );

        } else if ( rows.length > 0 ) {

          log.d( 'Looks like we the Oracle DB host System Date and Time: ' + rows[ 0 ] ); 
  
          response.result = rows[ 0 ];
          closeResultSet( rs );

        }   
      });
    }
  }

  // Get a pooled connection, check the Jde PDF process Queue, cleanup and return
  odb.getConnection( pool, processConnection );

}


// Add new JDE Job entry to F559811 JDE PDF Process Queue table
module.exports.addJobToProcessQueue = function( pool, dbc, row, cb ) {

  var jdeJobName = row[ 0 ];

  insertEntry( dbc );

  // Handle Insertion of new Jde Pdf Job to F559811 Jde Pdf Process Queue
  function insertEntry( dbc ) {

  var query,
    binds = [],
    options = { autoCommit: true };

    query = " INSERT INTO " + jdeEnvDb.trim() + ".F559811 VALUES (:jpfndfuf2, :jpsawlatm, :jpactivid, :jpyexpst, :jpblkk, :jppid, :jpjobn, :jpuser, :jpupmj, :jpupmt ) ";
    binds.push( row[ 0 ] );
    binds.push( audit.createTimestamp() );
    binds.push( hostname );
    binds.push( '100' );
    binds.push( row[ 1 ] + ' ' + row[ 2 ] );
    binds.push( 'PDFMONITOR' );
    binds.push( 'CENTOS' );
    binds.push( 'DOCKER' );
    binds.push( row[ 1 ] );
    binds.push( row[ 2 ] );

    // Insert entry into the F559811 DLINK Post PDF Handling Queue
    odb.performSQL( dbc, query, binds, options, function( err, result ) {

      if ( err ) {
      
        result = jdeJobName + ' INSERT FAILED ' + err;

      } else {

        result = jdeJobName + ' INSERTED' ;

      }

      log.i( result );
      return cb( null, result );

    });
  }
}
