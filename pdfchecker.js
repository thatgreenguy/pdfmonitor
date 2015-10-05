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

var  moment = require( 'moment' ),
  log = require( './common/logger.js' ),
  odb = require( './common/odb.js' ),
  pdfprocessqueue = require( './pdfprocessqueue.js' ),
  audit = require( './common/audit.js' ),
  numRows = 1,
  begin = null;


// Functions -
//
// module.exports.queryJdeJobControl = function(  dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, cb )


// Grab a connection from the Pool, query the database for the latest Queue entry
// release the connection back to the pool and finally return to caller with date/time of last entry
module.exports.queryJdeJobControl = function(  dbp, monitorFromDate, monitorFromTime, pollInterval, timeOffset, cb ) {

  var response = {},
    cn = null,
    checkStarted,
    checkFinished;

  checkStarted = new Date();
  response.error = null;
  response.result = null;


  log.d( 'Check Started: ' + checkStarted );

  // Grab a connection then query the F556110 JDE Job Control table, process any new entries tidy up and return
  odb.getConnection( dbp, processConnection );


  // Ensure Oracle resources released before handing response back to caller
  function releaseReturn() {

    if ( cn ) {

      odb.releaseConnection( cn, function( err, result ) {
 
        if ( err ) {

          log.e( 'Failed to release Oracle Connection back to Pool' );
          return cb( err );

        } else {

          log.d( ' Response Error: ' + response.error );
          log.d( ' Response Result: ' + response.result );
          log.d( 'Connection released back to Pool' );

          checkFinished = new Date();
          log.verbose( 'End Check: ' + checkFinished + ' took: ' + ( checkFinished - checkStarted ) + ' milliseconds' );

          return cb( response.error, response.result ); 

        }
      });    
    }
  }  


  function processConnection( err, connection ) {

    var query = null,
      binds = [],
      options = { resultSet: true },
      currentMomentAix,
      jdeTodayAix;

    if ( err ) {

      log.e( 'Failed to get a connection' );
      log.e( err );

      response.error = err;
      releaseReturn();
      
    } else {

      // Make returned connection available to other functions
      cn = connection;

      // We have monitorFromDate to build the JDE Job Control checking query, however, we need to also account for 
      // application startups that are potentially checking from a few days ago plus we need to account for when we are 
      // repeatedly monitoring (normal running mode) and we cross the midnight threshold and experience a Date change

      // Check the passed Monitor From Date to see if it is TODAY or not - use AIX Time not CENTOS
      currentMomentAix = moment().subtract( timeOffset); 
      jdeTodayAix = audit.getJdeJulianDateFromMoment( currentMomentAix );

      log.d( 'Check Date is : ' + monitorFromDate + ' Current (AIX) JDE Date is ' + jdeTodayAix );

//pjgtest switched chk
      if ( monitorFromDate !== jdeTodayAix ) {

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
        query += " WHERE ( ";
        query += " jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate = " + monitorFromDate + ' AND jcacttime >= ' + monitorFromTime;
        query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
        query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' )"
        query += " ) OR ( ";
        query += " jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate > " + monitorFromDate;
        query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
        query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' ) )";
        query += " ORDER BY jcactdate, jcacttime";
  
      }

      odb.performSQL( cn, query, binds, options, processResult );

    }
  }

  function processResult( err, rs ) {

    var icb,
      data = {};
 
    if ( err ) {

      log.e( 'Error detected performing Select query on JDE Job Control table' );
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
          releaseReturn();

        } else if ( rows.length == 0 ) {

          log.d( 'Finished processing JDE Job Control table no more records' );

          response.result = null;
          releaseReturn();

        } else if ( rows.length > 0 ) {

          log.d( 'New JDE Job Control entry found ...' );

          icb = function() { processResult( null, rs ); }

          pdfprocessqueue.addNewJdeJobToQueue( dbp, rows[ 0 ], icb  );

        }   
      });
    }
  }

}
