var audit = require( './common/audit.js' ),
  ts = null;

ts = audit.adjustTimestampByMinutes();


console.log( 'Timestamp: ' + ts.timestamp );
console.log( 'JDE Date: ' + ts.jdeDate );
console.log( 'JDE Time: ' + ts.jdeTime );


