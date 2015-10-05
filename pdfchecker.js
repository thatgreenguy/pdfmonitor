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
  audit = require( './common/audit.js' ),
  numRows = 1,
  begin = null;


// Functions -
//

// Query the JDE Job Control Master file to fetch all PDF files generated since last audit entry
// Only select PDF jobs that are registered for emailing
module.exports.queryJdeJobControl = function(  dbCn, monitorFromDate, monitorFromTime, pollInterval, hostname,
                                               lastPdf, timeOffset, cb ) {

  var query,
    currentMomentAix,
    jdeTodayAix,
    jdeNextDayAix;

  // We have monitorFromDate to build the JDE Job Control checking query, however, we need to also account for 
  // application startups that are potentially checking from a few days ago plus we need to account for when we 
  // repeatedly monitoring (normal running mode) and we cross the midnight threshold and experience a Date change

  // Check the passed Monitor From Date to see if it is TODAY or not use AIX Time not CENTOS
  currentMomentAix = moment().subtract( timeOffset); 
  jdeTodayAix = audit.getJdeJulianDateFromMoment( currentMomentAix );
  jdeNextDayAix = 

  log.d( 'Check Date is : ' + chkDate + ' Current (AIX) JDE Date is ' + currentJDE);

  // Determine if constructing a query in start up mode or normal monitoring mode 
  if ( monitorFromDate == jdeTodayAix ) {

    // On startup where startup is Today or whilst monitoring and no Date change yet
    // simply look for Job Control entries greater than or equal to monitorFromDate and monitorFromTime
     
    query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
    query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate >= ";
    query += chkDate + ' AND jcacttime >= ' + chkTime;
    query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
    query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' )";
    query += " ORDER BY jcactdate, jcacttime";

  else {

    // Otherwise Startup was before Today or we have crossed Midnight into a new day so query needs to adjust
    // and check for records on chkdate after chktime OR entries after chkDateNext

    query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
    query += " WHERE ( ";
    query += " jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate >= " + chkDate + ' AND jcacttime >= ' + chkTime;
    query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
    query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' )"
    query += " ) OR ( ";
    query += " jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate >= " + currentJdeDate;
    query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
    query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' ) )";
    query += " ORDER BY jcactdate, jcacttime";




  }




  // Checking of JDE Database Job Control is driven by passed Date and Time.
  // On startup where process has not run for a while this date and time could be from several days ago.
  // On subsequent calls (here) this date and time should be from a few seconds ago
  // i.e. first time could be looking for jobs going back several days that require processing but once
  // running it should be looking back and checking for the polling interval only

  // Get todays date from System in JDE Julian format
  jdeDateToday = audit.getJdeJulianDate();

  // If Passed check from Date is not today or we have just crossed midnight threshold the query should not use time 
  // as part of selection.

  if ( jdeDateToday == chkDate ) {
       
        query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
        query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate >= ";
        query += chkDate + ' AND jcacttime >= ' + chkTime;
        query += " AND RTRIM( SUBSTR( jcfndfuf2, 0, ( INSTR( jcfndfuf2, '_') - 1 )), ' ' ) in ";
        query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' )";
        query += " ORDER BY jcactdate, jcacttime";
    	
	log.debug( 'Check Date matches Todays Date : ' + jdeDateToday + ' see: ' + chkDate);

    } else { 
       
        query = "SELECT jcfndfuf2, jcactdate, jcacttime, jcprocessid FROM testdta.F556110 ";
        query += " WHERE jcjobsts = 'D' AND jcfuno = 'UBE' AND jcactdate >= ";
        query += chkDate;
        query += " AND RTRIM( SUBSTR(jcfndfuf2, 0, (INSTR(jcfndfuf2, '_') - 1)), ' ') in ";
        query += " ( SELECT RTRIM(crpgm, ' ') FROM testdta.F559890 WHERE crcfgsid = 'PDFMAILER' OR crcfgsid = 'PDFHANDLER' )";
        query += " ORDER BY jcactdate, jcacttime";
    	
	log.debug( 'Check Date is not today : ' + jdeDateToday + ' see: ' + chkDate);
    }

    log.debug(query);

    dbCn.execute( query, [], { resultSet: true }, function( err, rs ) {

        if ( err ) { 

          log.error( err.message );
          return;

        }

        processResultsFromF556110( 
          dbCn, rs.resultSet, numRows, begin, pollInterval, hostname, lastPdf, chkDate, chkTime, performPolledProcess );

    }); 
}


// Process results of query on JDE Job Control file 
function processResultsFromF556110( 
  dbCn, rsF556110, numRows, begin, pollInterval, hostname, lastPdf, chkDate, chkTime, sleepThenRepeat ) {

  var jobControlRecord,
  finish;

  rsF556110.getRows( numRows, function( err, rows ) {
    if ( err ) { 

      oracleResultsetClose( dbCn, rsF556110 );
      log.debug("rsF556110 Error");
      return;
	
    } else if ( rows.length == 0 ) {

      oracleResultsetClose( dbCn, rsF556110 );
      finish = new Date();
      log.verbose( 'End Check: ' + finish  + ' took: ' + ( finish - begin ) + ' milliseconds, Last Pdf: ' + lastPdf );
 
      // No more Job control records to process in this run - this run is done - so schedule next run
      sleepThenRepeat();

    } else if ( rows.length > 0 ) {

      jobControlRecord = rows[ 0 ];
      log.debug( jobControlRecord );

      // Process PDF entry
      processPdfEntry( dbCn, rsF556110, begin, jobControlRecord, pollInterval, hostname, lastPdf, chkDate, chkTime, sleepThenRepeat );            

    }
  }); 
}

// Called to handle processing of first and subsequent 'new' PDF Entries detected in JDE Output Queue  
function processPdfEntry( dbCn, rsF556110, begin, record, pollInterval, hostname, lastPdf, chkDate, chkTime, sleepThenRepeat ) {

  var cb = null,
    currentPdf,
    query,
    dt, 
    jdead,
    jdeat,
    jdeJobCompleted;

  dt = new Date();
  ts = audit.createTimestamp( dt );
  jdead = audit.getJdeJulianDate( dt );
  jdeat = audit.getJdeAuditTime( dt );
  jdeJobCompleted = record[ 1 ] + ' ' + record[ 2 ];

  currentPdf = record[ 0 ];
  log.verbose('Last PDF: ' + lastPdf + ' currentPdf: ' + currentPdf );

  // If Last Pdf is same as current Pdf then nothing changed since last check
  if ( lastPdf !== currentPdf ) {

    query = "INSERT INTO testdta.F559811 VALUES (:jpfndfuf2, :jpsawlatm, :jpactivid, :jpyexpst, :jpblkk, :jppid, :jpjobn, :jpuser, :jpupmj, :jpupmt )";
    log.debug( query );

    dbCn.execute( query,
      [ currentPdf, ts, hostname, '100', jdeJobCompleted, 'PDFMONITOR', 'CENTOS', 'DOCKER', jdead, jdeat ],
      { autoCommit: true },
      function( err, rs ) {

        // Once record written the check date and time should advance so in theory the process should not try
        // add add same record multiple times - but if this should happen its not an error.
        if ( err ) {
          log.debug( 'PDF already recorded in F559811 - This message can be ignored unless it happens continuously' );
          return;
        }

        log.info( 'New PDF ' + record[ 0 ] + ' added to F559811 Dlink Post PDF Process Queue' );    

      }
     );

  }

  // Process subsequent PDF entries if any - Read next Job Control record
  processResultsFromF556110( dbCn, rsF556110, numRows, begin, pollInterval, hostname, lastPdf, chkDate, chkTime, sleepThenRepeat );

}
