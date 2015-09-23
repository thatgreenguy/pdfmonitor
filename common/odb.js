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
module.exports.createPool = function( ) {

}


//
// Terminate a connection Pool.
module.exports.terminatePool = function( ) {

}


//
// Get a connection from the Pool.
module.exports.getConnection = function( ) {

}


//
// Release a connection back to the Pool.
module.exports.releaseConnection = function( ) {

}


//
// Perform an Insert query statement.
module.exports.performInsert = function( ) {

}


//
// Perform a select query statement.
module.exports.performDelete = function( ) {

}


//
// Perform a select query statement.
module.exports.performSelect = function( ) {

}


//
// Close a selection resultset.
module.exports.closeSelectResult = function( ) {

}
