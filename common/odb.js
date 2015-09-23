// Module		: odb.js
// Description		: Common oracle database related functionality.
// Author		: Paul Green
// Dated		: 2015-09-23
//
// Use this module to create a connection pool, create a connection, execute passed insert statements, and run selects
  

var oracledb = require( 'oracledb' ),
  log = require( './logger' ),
  credentials = { user: process.env.DB_USER, password: process.env.DB_PWD, connectString: process.env.DB_NAME },
  poolMax = 4,
  poolMin = 0,
  poolIncrement = 1,
  poolTimeout = 0;


// - Values
//
module.exports.poolMax;
module.exports.poolMin;
module.exports.poolIncrement;
module.exports.poolTimeout;


// - Initialisation
//



// - Functions
//
// createPool
// terminatePool
// getConnection
// releaseConnection
// performInsert
// performDelete
// performSelect
// closeSelectResult

//
// Create a connection Pool.
module.exports.createPool = function( cb ) {

  oracledb.createPool( credentials, function( err, cb ) {

    if ( err ) {
 
      log.error( 'createPool: FAILED: to create a new oracle DB connection Pool' );
      log.error( err );
      cb( err, null );      

    } else {

      log.debug( 'createPool: OK' );
      log.debug( pool );
      cb( null, pool );      

    }
  }
}


//
// Terminate a connection Pool.
module.exports.terminatePool = function( pool, cb ) {

  pool.terminate( function( err ) {

    if ( err ) {

      log.error( 'terminatePool: FAILED: to terminate the passed connection pool' );
      log.error( err );
      cb( err );

    } else {

      log.debug( 'terminatePool: OK' );
      cb( null );

    }
  }
}


//
// Get a connection from the Pool.
module.exports.getConnection = function( pool, cb ) {

  pool.getConnection( function( err, connection ) {

    if ( err ) {

      log.verbose( 'getConnection: FAILED: to get a Connection from the passed Pool' );
      log.error( err );
      cb( err, null );
 
    } else {

      log.debug( 'getConnection: OK' );
      cb( null, connection ); 

    }
  });
}


//
// Release a connection back to the Pool.
module.exports.releaseConnection = function( connection, cb ) {

  connection.release( function( err ) {

    if ( err ) {

      log.error( 'releaseConnection: FAILED: to release connection' );
      log.error( err );
      cb( err, null );

    } else {

      log.debug( 'releaseConnection: OK' );
      cb( null ); 

    }
  });
}


//
// Perform an adhoc SQL query Statement.
module.exports.performSQL = function( connection, query, binds, options, cb ) {

  log.debug( 'performSQL: ' + query );

  connection.execute( queryStatement, binds, options, function( err, result ) {

    if ( err ) {

      log.warn( 'performSQL FAILED : ' );
      log.error( err );
      cb( err, null );  

    } else {

      log.debug( 'performSQL: OK' );
      cb( null, result ); 

    }
  });
}




//
// Perform a select query statement.
module.exports.performSelect = function( connection, cb ) {

  log.debug( 'performSelect: ' + sql );

  connection.execute( sql, binds, options, function( err, rs ) {

    if ( err ) {

      log.warn( 'performSelect: FAILED : ' );
      log.error( err );
      cb( err, null );  

    } else {

      log.debug( 'performSelect: OK' );
      cb( null, result ); 

    }
  });
}


//
// Close the passed selection result set.
module.exports.closeSelectSet = function( connection, rs, cb ) {

  rs.close( function( err ) {

    if ( err ) {

      log.error( 'closeSelectSet: FAILED : ' );
      log.error( err );
      module.exports.releaseConnection( connection, cb );

    } else {

      log.debug( 'closeSelectSet: OK ' );
      cb( null );

    }
  });
}
