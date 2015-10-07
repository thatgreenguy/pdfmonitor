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
  hostname = process.env.HOSTNAME;


// Functions -
//


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

          log.e( 'Failed to release Oracle Connection back to Pool' );
          return cb( err );

        } else {

         log.d( ' Response Error: ' + response.error );
         log.d( ' Response Result: ' + response.result );
         log.d( 'Connection released back to Pool' );

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
 
      query = 'SELECT jpfndfuf2, jpblkk FROM testdta.F559811 ORDER BY jpsawlatm DESC';

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

          log.w( 'No records found JDE process Queue is empty' );

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

         log.d( ' Response Error: ' + response.error );
         log.d( ' Response Result: ' + response.result );
         log.d( 'Connection released back to Pool' );

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


// This function accepts a new Jde PDF Job and adds it to the F559811 JDE PDF Process Queue
// Where it will be picked up by the Logo / and or Email PDF handler programs for further processing
//
// Note: Usually this monitor process will be finding 1 or 2 entries on any given run but occasionally - say on startup
// after the application has been offline for some time there could be many multiple JDE PDF entries to be processed.
// As Oracle DB Connections are a limited resource and when processing lots of entries very quickly the connection pool 
// could be busy - so if no connection is available immediately the function simply schedules itself to run again on the next tick
// of the event loop (after I/O not before!) 
module.exports.processNewJdeJobToQueue = function( pool, row ) {

  var query,
    binds = [],
    options = { maxRows: 1 },
    dbc,
    selectResult,
    jdeJobName = row[ 0 ];


  // Handle Insertion of new Jde Pdf Job to F559811 Jde Pdf Process Queue
  function insertEntry( dbc ) {

  var query,
    binds = [],
    options = { autoCommit: true };

    query = ' INSERT INTO testdta.F559811 VALUES (:jpfndfuf2, :jpsawlatm, :jpactivid, :jpyexpst, :jpblkk, :jppid, :jpjobn, :jpuser, :jpupmj, :jpupmt )  ';
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
    odb.performSQL( dbc, query, binds, options, function( err, row ) {

      if ( err ) {

        log.w( row[ 0 ] + ' FAILED INSERT to F559811 process queue' + err );

        // releaseReturn();

      } else {

        log.v( row[ 0 ] + ' INSERTED to F559811 process Queue' );
  
      }
    });
  }


  // Release connection back to pool
  function releaseConnection( dbc ) { 
    
    dbc.release( function( err ) {
    
      if ( err ) {

        log.d( 'Error releasing connection back to pool ' + err );
    
      }
    });
  }


  // Close query result set 
  function closeResultSet( dbc, rs ) {

    odb.closeSelectSet( dbc, rs, function( err ) { 

      if ( err ) {
       
        log.e( 'Error closing result set in processNewJdeJobToQueue' + err );
      }
    });
  }


  // Get a connection check entry not already in process queue then add it
  odb.getConnection( pool, function( err, dbc ) {

    if ( err ) {

      // If failed to get a connection - most likely pooled connections are all in use so 
      // retry shortly on next tick of event loop (after I/O)
      log.v( 'No Pool DB Connections available - retry again shortly' ); 

      // ----------- for now just return!!!
      return;
 
    } else {

      // Have connection so check if entry already exists before attempting insert
      // Only want to see insert errors if genuine - multiple instances of this process could be running
      // for redundancy so only attempt insert if not already added to process queue

      query = "SELECT COUNT(*) AS checkCount FROM testdta.F559811 WHERE jpfndfuf2 = ";
      query += "'" + jdeJobName + "'"

      dbc.execute( query, binds, options, function( err, rs ) {

        if ( err ) { 

          log.e( 'Got connection but Query Failed so Abort - will be picked up on next run and retried' );
      
          releaseConnection( dbc )
          return;
 
        } else {

          // Grab query results then release the result set as no longer required
          checkCount = rs.row[ 0 ];
          closeResultSet( dbc, rs );          

          // Determine whether we should add this JDE Job to F559811 JDE PDF Process Queue  or not
          if ( checkCount == '0' ) {

            insertEntry();
      
          } else {

            log.d( jdeJobName + ' Already in F559811 Process Queue - Ignored' );

          }
        }
      });

    // Whatever happens make sure to release the connection back to the Pool
    releaseConnection( dbc );     

    }
  });
}
