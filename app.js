'use strict';

const util  = require('util');
const Async = require('async');
const _     = require('lodash');
const net   = require('net');
const Invoker = require('./lib/kinesis_stream_fetch_invoker_service.js');

const server = net.createServer((connection) => {

    connection.on('end', () => {

        console.log('Client disconnected');
    })

    connection.on('listening', (data) => {

        console.log('Listening...', data);
    })

})


server.listen(9020, () => {
    console.log(server);
    console.log('Server listening on ', 9020);
    Invoker.fetch();
})


