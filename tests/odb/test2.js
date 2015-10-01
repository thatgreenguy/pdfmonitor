var odb = require( './odb.js' ),
  async = require( 'async' );

var pool,
  conn,
  rs,
  sql,
  bind,
  options = { autoCommit: true };


// Create a Pool 
odb.createPool( gotPool );



function gotPool( err, pool) {

  if ( err ) { 
    console.log('NAH');
  } else {
    console.log('OK');
    getSeveralConn( pool, 4 );
  }
}

function getSeveralConn( pool, x ) {

  var i = 0,
    cb = null;

  // Get and release x connections form Pool
  for ( i=0; i < x ; i++ ) {
    
    odb.getConnection( pool, gotConnection ) {

    }
  }
}


function gotConnection( err, pool ) {

  if ( err ) {

    console.log( 'Error: get Connection from Pool Failed' );

  } else {

    console.log( 'Got a Connection : ' + connection;
    console.log( 'On Pool : ' + pool;

    odb.releaseConnection( connection, connectionReleased );
  }
}


function connectionReleased( err ) {

  if ( err )  {

    console.log( 'NAH Connection Not released' );

  } else {

    console.log( 'OK Connection Released ');
  }

}
