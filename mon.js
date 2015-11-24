var oracledb = require( 'oracledb' ),
  log = require( './common/logger' );
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  hostname = process.env.HOSTNAME,
  jdeDB = process.env.JDEDB,
  jdeEnv = process.env.JDE_ENV,
  jdeEnvDb = process.env.JDE_ENV_DB;






// Establish connection, insert new entry to process queue then release connection
function queueNewPdfEntry( data, cb ) {

  var sql, 
  binds = [], 
  options = { autoCommit:true };

  // Get a database connection
  oracledb.getConnection( credentials, function( err, conn ) {
    if ( err ) {

      log.e( 'Unable to get a connection' + err );
      return cb( err ); 

    } else {

      // Perform insert using new connection
      sql = " INSERT INTO " + jdeEnvDb.trim() + ".F559811 VALUES (:jpfndfuf2, :jpsawlatm, :jpactivid, :jpyexpst, :jpblkk, :jppid, :jpjobn, :jpuser, :jpupmj, :jpupmt ) ";
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
      conn.execute( sql, binds, options, function( err, result ) {

        if ( err ) {

          log.e( 'Failed to insert new PDF to Queue' + err );
          conn.release( function( err ) { 
            if ( err ) {
              log.e( 'Failed to release connection ' + err );
              return cb( err );      
            }
          });
  
          releaseConnection( conn );
          return cb( err );

        } else {


        }




      });
     }
  });

}





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


