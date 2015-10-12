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
 
      query = 'SELECT jpfndfuf2, jpblkk FROM testdta.F559811 ORDER BY jpupmj DESC, jpupmt DESC';

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

          log.i( 'No records found in JDE process Queue table has been cleared / is empty' );

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

  log.d( 'INSERTING row : ' + row );

  insertEntry( dbc );


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
