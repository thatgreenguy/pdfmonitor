// pdfchecker.js  : Check Jde Job Control table looking for any recently generated Pdf files that are configured 
//                : in JDE for some kind of post Pdf processing when found add them to process Queue.
// Author         : Paul Green
// Dated          : 2015-09-21
//
// Synopsis
// --------
//
// Called periodically by pdfmonitor.js
// Checks the Jde Job Control table looking for recently completed UBE reports.
// New PDF files are cross checked against JDE Post PDF handling setup/configuration file in JDE 
// and if some kind of post pdf processing is required e.g. Logos or Mailing then the Jde Job is added to
// the F559811 DLINK Post PDF Handling Queue


var moment = require( 'moment' ),
  async = require( 'async' ),
  log = require( './common/logger.js' ),
  odb = require( './common/odb.js' ),
  pdfprocessqueue = require( './pdfprocessqueue.js' ),
  audit = require( './common/audit.js' ),
  jdeEnv = process.env.JDE_ENV
  jdeEnvDb = process.env.JDE_ENV_DB;
  jdeEnvDbF556110 = process.env.JDE_ENV_DB_F556110;
  

// Functions -
//
// module.exports.queryJdeJobControl = function(  dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, cb )
// function constructQuery( monitorFromDate, monitorFromTime, timeOffset )


// Grab a connection from the Pool, query the database for the latest Queue entry
// release the connection back to the pool and finally return to caller with date/time of last entry
module.exports.queryJdeJobControl = function(  dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, lastJdeJob, cb ) {

  var p;

  p = { 'pool': dbp, 'monitorFromDate': monitorFromDate, 'monitorFromTime': monitorFromTime, 'timeOffset': timeOffset, 'lastJdeJob': lastJdeJob, 'cb': cb };

  log.v( p.lastJdeJob + ' : Perform Check for new PDF\'s' );

  async.series([
    function( next ) { reportPoolStats( p, next ) },
    function( next ) { getConnection( p, next ) },
    function( next ) { performQuery( p, next ) },
    function( next ) { processRecords( p, next ) }
  ], function( err, res ) {

    if ( err ) {

      log.d( p.lastJdeJob + ' : Async series error : ' + err );

    }
    closeReleaseReturn( p );

  });
}


// Get database connection from database pool to use for this query check  
function reportPoolStats( p, cb ) {

  log.v( p.lastJdeJob + ' : Report Pool Stats' );

  log.v( 'Connections in use : ' + p.pool.connectionsInUse );
  log.v( 'Connections open   : ' + p.pool.connectionsOpen );

  return cb( null );

}


// Get database connection from database pool to use for this query check  
function getConnection( p, cb ) {

  log.v( p.lastJdeJob + ' : Get Connection' );

  odb.getConnection( p.pool, function( err, dbc ) {

    if ( err ) {

      log.d( p.lastJdeJob + ' : getConnection Failed : ' + err );     
      return cb( err );

    } else {

      p.dbc = dbc;
      return cb( null );

    }

  });
}


// execute the required query
function performQuery( p, cb ) {

  var querySql,
    binds = [],
    options = { resultSet: true };

  log.v( p.lastJdeJob + ' : Perform Query' );
  querySql = constructQuery( p.monitorFromDate, p.monitorFromTime, p.timeOffset );

  p.dbc.execute( querySql, binds, options, function( err, res ) {

    if ( err ) {

      log.e( p.lastJdeJob + ' : Error on Perform Query' + err );
      return cb( err );
 
    } else {

      p.rs = res.resultSet;
      return cb( null );

    }
  });
}


// Process all results from query 
function processRecords( p, cb ) {

  var asyncCb;

  asyncCb = cb;

  log.v( p.lastJdeJob + ' : Process Query Results' );

  // Don't know how many rows exactly will be read so read in blocks of 10 and recurse until all done 
  // (usually 1 or maybe 2 or 3 PDF entries each cycle but on startup could be quite a few)
  function fetchNextRow( p, asyncCb ) {

    p.rs.getRows( 1, function( err, rows ) { 

      if ( err ) {

        log.e( p.lastJdeJob + ' : Error reading resultset ' );
        return asyncCb( err );

      } else if ( rows.length == 0 ) { 

        log.d( p.lastJdeJob + ' : No more rows to read ' );
        return asyncCb( null );

      } else if ( rows.length > 0 ) {

        log.d( p.lastJdeJob + ' : Read row ' + rows[ 0 ] );
   
        // Process rows read (new Pdf entries)     
        processNewPdf( p, rows[ 0 ], function( err, res ) {

          if ( err ) {

            log.e( p.lastJdeJob + ' Error processing new PDF Entry ' + err );
            return asyncCb( err );

          } else {

            log.v( p.lastJdeJob + ' Processed new PDF Entry ' + rows[ 0 ] );
            fetchNextRow( p, asyncCb )

          }
        });
      }
    });
  }

  // Start fetching and processing rows from query - fetchNextRow calls itself until no more rows! 
  fetchNextRow( p, cb );

}


// Process each record record
function processNewPdf( p, row, cb ) {

  log.d( p.lastJdeJob + ' Processing new Pdf row : ' + row );  

  // If the last known PDF processed matches current PDF entry then we can ignore it
  if ( row[ 0 ] == p.lastJdeJob ) {
  
    log.d( 'This PDF : ' + row[ 0 ] + ' has already been processed - Ignore it' );
    return cb( null );

  } else {

    // Check this PDF definitely not already added to PDF process Queue before attempting insert
    // if several jobs happen to finish at same time e.g. R4210IC overnight jobs often do)
    // query check may have picked up say 3 rows that have already been processed and above
    // check only identifies one of them - so need to check DB before trying Insert

    pdfprocessqueue.getPdfEntry( p.pool, row[ 0 ], function( err, result ) {

      if ( err ) {

        log.e( 'Error checking if this PDF : ' + row[ 0 ] + ' is already in F559811' );
        return cb( err );
        
      } else {

        if ( result !== null ) {

          log.d( ' PDF : ' + row[ 0 ] + ' already added to the JDE PDF Process Queue F559811 - Ignore!' );
          return cb( null );

        } else {

          log.d( ' PDF : ' + row[ 0 ] + ' not yet added to JDE PDF Process Queue F559811 - Process / Add it now' );
 
          // Hand over this row to be added to the F559811 
          // No callback if the Select Check/Insert combination fails it will be picked up again and retried on 
          // a later run!
          pdfprocessqueue.addJobToProcessQueue( p.pool, p.dbc, row, function( err, result ) {

            if ( err ) {

              log.d( ' PDF : ' + row[ 0 ] + ' Error trying to insert - will retry on next run!' );
              return cb( err );

            } else {

              log.i( ' PDF : ' + row[ 0 ] + ' Added to JDE PDF Process Queue' );
              return cb( null );

            }
          });      
        }
      }
    });
  }
}



// Close resultset, release connection then return to caller
function closeReleaseReturn( p, cb ) {

  log.v( p.lastJdeJob + ' : Close, Release and Return' );

  async.series([
    function( next ) { closeResultSet( p, next ) }
  ], function( err, res ) {

    if ( err ) {

      log.d( p.lastJdeJob + ' : Async series error : ' + err );

    }

    releaseConnection( p );

  });
}


// Close result set 
function closeResultSet( p, cb ) {

  log.v( p.lastJdeJob + ' : Close Result Set' );


  // If we have a result set then close it
  if ( p.rs ) {
    p.rs.close( function( err ) {             

      if ( err ) {

        log.e( 'Error closing result set: ' + err );
        return cb( err );
      
      } else {

        log.d( p.lastJdeJob + ' : Result Set Closed ' );
        return cb( null );
      }
    });

  // Otherwise ...
  } else {

    log.d( p.lastJdeJob + ' : No Result Set To Close? ' );
    return cb( null );

  }
}


// Release Connection and return to caller 
function releaseConnection( p, cb ) {

  log.v( p.lastJdeJob + ' : Release Connection back to pool' );

  // If we have a connection then close it
  if ( p.dbc ) {
    p.dbc.release( function( err ) {

      if ( err ) {

        log.e( 'Error releasing connection : ' + err );
        return p.cb( err );

      } else {

        // Pass control back to caller
        log.d( p.lastJdeJob + ' : Connection released back to pool ' );
        log.d( ' ' );
        return p.cb( null );
      }
    });
  
  // Otherwise ...
  } else {

    // Pass control back to caller
    return p.cb( null );
  }
}





// Grab a connection from the Pool, query the database for the latest Queue entry
// release the connection back to the pool and finally return to caller with date/time of last entry
module.exports.OLDqueryJdeJobControl = function(  dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, lastJdeJob, cb ) {

  var response = {},
  cn = null,
  checkStarted,
  checkFinished,
  duration,
  query,
  binds = [],
  rowBlockSize = 5,
  options = { resultSet: true, prefetchRows: rowBlockSize };

  response.error = null;
  response.result = null;
  checkStarted = new Date();


  query = constructQuery( monitorFromDate, monitorFromTime, timeOffset );

  log.d( 'Check Started: ' + checkStarted );

  odb.getConnection( dbp, function( err, dbc ) {

    if ( err ) {
      log.w( 'Failed to get an Oracle connection to use for F556110 Query Check - will retry next run' );
      return cb;
    }

    dbc.execute( query, binds, options, function( err, results ) {

      if ( err ) throw err;   

      // Recursivly process result set until no more rows
      function processResultSet( dbc ) {

        results.resultSet.getRows( rowBlockSize, function( err, rows ) {

          if ( err ) {
            log.w( 'Error encountered trying to query F556110 - release connection and retry ' + err );
            dbc.release( function( err ) {

              log.d( 'Error releasing F556110 Connection: ' + err );

              // Once connection release can return to continue monitoring
              return cb( null, dbp );

            });
          }

          if ( rows.length ) {
            
            processRows( dbp, dbc, rows, lastJdeJob );

            // Process subsequent block of records
            processResultSet( dbc ); 
 
            return;

          }


          checkFinished = new Date();
          log.d( 'Check Finished: ' + checkFinished + ' took ' + (checkFinished - checkStarted) + ' milliseconds' );

          results.resultSet.close( function( err ) { 
          if ( err ) log.d( 'Error closing F556110 result set: ' + err );

            dbc.release( function( err ) {
            if ( err ) log.d( 'Error releasing F556110 Connection: ' + err );

            // Once connection release can return to continue monitoring
            return cb( null, dbp );

            });
          });
        });
      }

      // Process first block of records
      processResultSet( dbc );


    });
  });
}


// Process block of rows - need to check and insert each row into F559811 JDE PDF Process Queue
function processRows ( dbp, dbc, rows, lastJdeJob ) { 

  if ( rows.length ) {

    rows.forEach( function( row ) {

      // If the current row is the last Jde Job successully processed into the F559811 then we do not need to 
      // process that one again.
      if ( lastJdeJob == row[ 0 ] ) {

        log.d( 'Ignoring ' + lastJdeJob + row[ 0 ] + ' as processed in previous run' );

      } else {

        // Hand over this row to be added to the F559811 
        // No callback if the Select Check/Insert combination fails it will be picked up again and retried on 
        // a later run!
        pdfprocessqueue.addJobToProcessQueue( dbp, dbc, row, function( err, result ) {
 
          if ( err ) {

            rowFailure( dbp, dbc, row, err, result );

          } else { 

            rowSuccess( dbp, dbc, row, err, result );

          }
        });   
      }
    });
  }
}


function rowFailure( dbp, dbc, row, err, result ) {

  log.e( row + ' Process Row Failed - Not sure how to recover report and bail!' );
  log.e( row + ' : ' + result );


}


function rowSuccess( dbp, dbc, row, err, result ) {

  log.i( ' PDF Entry added to Queue : ' + result );

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
