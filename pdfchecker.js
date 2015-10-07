// pdfchecker.js  : Check Jde Job Control table looking for any recently generated Pdf files that are configured 
//                : in JDE for some kind of post Pdf processing when found add them to process Queue.
// Author         : Paul Green
// Dated          : 2015-09-21
//
// Synopsis
// --------
//
// Called periodically by pdfmonitor.js
// It checks the Jde Job Control Audit table looking for recently completed UBE reports.
// New PDF files are cross checked against JDE email configuration and if some kind of post pdf processing is required
// e.g. Logos or mailing then the Jde Job is added to the F559811 DLINK Post PDF Handling Queue

var moment = require( 'moment' ),
  log = require( './common/logger.js' ),
  odb = require( './common/odb.js' ),
  pdfprocessqueue = require( './pdfprocessqueue.js' ),
  audit = require( './common/audit.js' );
  

// Functions -
//
// module.exports.queryJdeJobControl = function(  dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, cb )


// Grab a connection from the Pool, query the database for the latest Queue entry
// release the connection back to the pool and finally return to caller with date/time of last entry
module.exports.queryJdeJobControl = function(  dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, lastJdeJob, cb ) {

  var response = {},
  cn = null,
  checkStarted,
  checkFinished,
  duration,
  query,
  binds = [],
  rowBlockSize = 10,
  options = { resultSet: true, prefetchRows: rowBlockSize };

  response.error = null;
  response.result = null;
  checkStarted = new Date();


  query = constructQuery( monitorFromDate, monitorFromTime, timeOffset );

  log.d( 'Check Started: ' + checkStarted );


  odb.getConnection( dbp, function( err, dbc ) {

    if ( err ) {
      log.w( 'Failed to get an Oracle connection to use for F556110 Query Check' );
      return cb;
    }

    dbc.execute( query, binds, options, function( err, results ) {

      if ( err ) throw err;   

      // Recursivly process result set until no more rows
      function processResultSet() {

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
            
            processRows( dbp, rows, lastJdeJob );

            // Process subsequent block of records
            processResultSet(); 
 
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
      processResultSet();


    });
  });
}


// Process block of rows - need to check and insert each row into F559811 JDE PDF Process Queue
function processRows ( dbp, rows, lastJdeJob ) { 

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
        pdfprocessqueue.addJobToProcessQueue( dbp, row, function( err, result ) {
 
          if ( err ) {

            rowFailure( dbp, row, err, result );

          } else { 

            rowSuccess( dbp, row, err, result );

          }
        });   
      }
    });
  }
}


function rowFailure( dbp, row, err, result ) {

  log.i( row + ' Process Row Failed - Should we retry?' );
  log.i( row + ' : ' + result );


}


function rowSuccess( dbp, row, err, result ) {

  log.i( row + ' Done ' );
  log.i( row + ' : ' + result );

}



// Perform processing on each row

//          if ( lastJdeJob == rows[ 0 ][ 0 ] ) {
//            log.d( 'This JDE Job already processed - ignore it : ' + rows[ 0 ][ 0 ] );
//            processResult( null, rs ); 
//         } else {
//            log.d( 'Add this new JDE Job to the F559811 Process Queue : ' + rows[ 0 ][ 0 ] );
//            icb = function() { processResult( null, rs ); }
//            pdfprocessqueue.addNewJdeJobToQueue( dbp, rows[ 0 ], icb  );
//          }


// Construct query which is suitable for monitor from date and time and considering
// change of day on both startup and crossing midnight boundary
function constructQuery( monitorFromDate, monitorFromTime, timeOffset ) {

  var query = null,
      currentMomentAix,
      jdeTodayAix;

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
     
    query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
    query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' "
    query += " AND jcactdate = " + monitorFromDate + ' AND jcacttime >= ' + monitorFromTime;
    query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
    query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' )";
    query += " ORDER BY jcactdate, jcacttime";

  } else {

    // Otherwise Startup was before Today or we have crossed Midnight into a new day so query needs to adjust
    // and check for records on both sides of the date change

    query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
    query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' "
    query += " AND (( jcactdate = " + monitorFromDate + " AND jcacttime >= " + monitorFromTime + ") ";
    query += " OR ( jcactdate > " + monitorFromDate + " )) ";
    query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
    query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' ) ";
    query += " ORDER BY jcactdate, jcacttime";
  
  }

  log.d( query );

  return query;

}
