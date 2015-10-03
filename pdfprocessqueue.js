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
  jdeDB = process.env.JDEDB;


// Functions -
//


// Grab a connection from the Pool, query the database for the latest Queue entry
// release the connection back to the pool and finally return to caller with date/time of last entry
module.exports.getLatestQueueEntry = function( pool, cb ) {

  var response = {};

  response.error = null;
  response.data = null;

  function processConnection( err, connection ) {

    var query = null,
      binds = [],
      options = { resultSet: true };

    if ( err ) {

      log.e( 'Failed to get a connection' );
      log.e( err );
      
      return cb( err );

    } else {

      query = 'SELECT jpfndfuf2, jpblkk FROM testdta.F559811 ORDER BY jpsawlatm DESC';

      odb.performSQL( connection, query, binds, options, processResult );

    }
  }

  function processResult( err, rs ) {

    if ( err ) {

      log.e( 'Failed to get a Select result' );
      log.e( err );

      response.error = err;
      response.result = null;
      
      cleanupAndReturn();

    } else {

      rs.resultSet.getRows( 1, function( err, rows ) {

        if ( err ) {

          log.e( 'Failed to get Row fro resultset' );
          log.e( err );

          odb.releaseConnection( connection, cleanupAndReturn );    

        } else if ( rows.length == 0 ) {

          response.result = 'System Date and Time';

          log.w( 'No records found JDE process Queuee is empty' );
          odb.releaseConnection( connection, cleanupAndReturn );    

        } else if ( rows.length > 0 ) {

          log.d( 'We have latest processed entry from queue: ' + rows[ 0 ] ); 
  
          response.result = rows[ 0 ];

          cleanupAndReturn();

        }   
      });
    }
  }


  function cleanupAndReturn() {

    log.d( ' Release Oracle DB resources and return' );
    log.d( ' Response Error: ' + response.error );
    log.d( ' Response Result: ' + response.result );

    return cb( response.error, response.result );

  }  

  // Get a pooled connection, check the Jde PDF process Queue, cleanup and return
  odb.getConnection( pool, processConnection );

}
