// Module		: logger.js
// Description		: Common application logging.
// Author		: Paul Green
// Dated		: 2015-09-03
//
//
// 

var winston = require('winston');

winston.emitErrs = true;

var logger = new winston.Logger({
    transports: [
        new winston.transports.File({
            level: 'debug',
            filename: './src/logs/pdfmonitor.log',
            handleExceptions: true,
            json: true,
            maxsize: 54558720,
            maxFiles: 7,
            colorize: false 
        }),
        new winston.transports.Console({
            level: 'debug',
            handleExceptions: true,
            json: false,
            colorize: true
        })
    ],
    exitOnError: false
});

module.exports = logger;
module.exports.stream = {
    write: function(message, encoding) {
        logger.info(message);
    }
};


// Simply handles logging DEBUG messages prepending timestamp
module.exports.d = function( str ) {

  logger.debug( now() + ' ' + str );

} 

// Simply handles logging INFO messages prepending timestamp
module.exports.i = function( str ) {

  logger.info( now() + ' ' + str );

} 

// Simply handles logging VERBOSE messages prepending timestamp
module.exports.v = function( str ) {

  logger.verbose( now() + ' ' + str );

} 


// Simply handles logging WARN messages prepending timestamp
module.exports.w = function( str ) {

  logger.warn( now() + ' ' + str );

} 


// Simply handles logging ERROR messages prepending timestamp
module.exports.e = function( str ) {

  logger.error( now() + ' ' + str );

} 

// Return Timestamp for console message display
function now() {

  return Date();

}
