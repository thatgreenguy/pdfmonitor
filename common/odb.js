// Module		: odb.js
// Description		: Common oracle database related functionality.
// Author		: Paul Green
// Dated		: 2015-09-23
//
// Use this module to create a connection pool, create a connection, execute passed insert statements, and run selects
  

var oracledb = require( 'oracledb' ),
  log = require( './logger' ),
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  poolMax = 5,
  poolMin = 3,
  poolIncrement = 1,
  poolTimeout = 60;


// - Functions
//
// createPool
// terminatePool
// getConnection
// releaseConnection
// performSQL
// closeSelectResult

//
// Create a connection Pool.
module.exports.createPool = function( cb ) {

  oracledb.createPool( credentials, function( err, pool ) {

    if ( err ) {
 
      log.e( 'createPool: FAILED: to create a new oracle DB connection Pool' + err );
      return cb( err, null );      

    } else {

      log.d( 'createPool: OK' );
      return cb( null, pool );      

    }
  });
}


//
// Terminate a connection Pool.
module.exports.terminatePool = function( pool, cb ) {

  pool.terminate( function( err ) {

    if ( err ) {

      log.e( 'terminatePool: FAILED: to terminate the passed connection pool : ' + err );
      return cb( err );

    } else {

      log.d( 'terminatePool: OK' );
      return cb( null );

    }
  });
}


//
// Get a connection from the Pool.
module.exports.getConnection = function( pool, cb ) {

  pool.getConnection( function( err, connection ) {

    if ( err ) {

      log.v( 'getConnection: FAILED: to get a Connection from the passed Pool : ' + err );
      return cb( err );
 
    } else {

      log.d( 'getConnection: OK' );
      return cb( null, connection ); 

    }
  });
}


//
// Release a connection back to the Pool.
module.exports.releaseConnection = function( connection, cb ) {

  connection.release( function( err ) {

    if ( err ) {

      log.e( 'releaseConnection: FAILED: to release connection : ' + err );
      return cb( err, null );

    } else {

      log.d( 'releaseConnection: OK' );
      return cb( null ); 

    }
  });
}


//
// Perform an adhoc SQL query Statement.
module.exports.performSQL = function( connection, query, binds, options, cb ) {

  var executeQuery = null;

  connection.execute( query, binds, options, function( err, result ) {

    if ( err ) {

      log.e( 'performSQL FAILED : ' + query + ' ERROR: ' + err );
      log.d( 'performSQL Binds: ' + binds );
      log.d( 'performSQL Options: ' + options );
      return cb( err, null );  

    } else {

      log.d( 'performSQL OK : ' + query );
      return cb( null, result ); 

    }
  });
}


//
// Close the passed selection result set.
module.exports.closeSelectSet = function( connection, rs, cb ) {

  rs.resultSet.close( function( err ) {

    if ( err ) {

      log.e( 'closeSelectSet: FAILED : ' );
      log.e( err );
      return module.exports.releaseConnection( connection, cb );

    } else {

      log.d( 'closeSelectSet: OK ' );
      return cb( null );

    }
  });
}
