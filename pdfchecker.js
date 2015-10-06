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
  async = require( 'async' ),
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
  options = { resultSet: true },
  asyncQ;

  response.error = null;
  response.result = null;
  checkStarted = new Date();


  asyncQ = async.queue( function ( row , cb ) {
   
   log.v( 'Jde Job ' + row + ' scheduled for processing ' ); 
 
   pdfprocessqueue.processNewJdeJobToQueue( dbp, row[ 0 ], function( err, results ) {

     if ( err ) {

       log.w( 'JDE Job ' + row[ 0 ] + ' processing Failed: ' + err );
       log.w( 'Going back on Queue for another attempt' );
       

     } else {

       log.i( 'JDE Job ' + row[ 0 ] + ' PROCESSED OK: ' + results.toString() );

     }
   });
    
   return cb();

  }, 4 );

  query = constructQuery( monitorFromDate, monitorFromTime, timeOffset );

  log.d( 'Check Started: ' + checkStarted );


  odb.getConnection( dbp, function( err, dbc ) {

    if ( err ) throw err;

    dbc.execute( query, binds, options, function( err, results ) {

      var rowCount = 0;
      if ( err ) throw err;   

      function processResultSet() {

        results.resultSet.getRow( function( err, row ) {

          if ( err ) throw err;
          if ( row ) {

            log.i( 'Push this row to Queue: ' + row );

            // Stick this row onto the Queue to process asynchronously without breaking DB connection limit
            asyncQ.push( [ row ] );
            rowCount += 1;

            processResultSet(); 

            return;

          }

          log.d( 'F556110 Entries processed: ' + rowCount );
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

      processResultSet();

    });
  });
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
