var odb = require( './odb.js' );

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
    dropPool( pool );
  }
}

function dropPool( pool ) {

  odb.terminatePool( pool, allDone );

}


function allDone( err ) {

  if ( err )  {

    console.log( 'NAH' );

  } else {

    console.log( 'OK');
  }

}
