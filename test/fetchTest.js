'use strict';

let streamConfig  = require('./stream_config.js');

const util  = require('util');
const _     = require('lodash');
const net   = require('net');
const Invoker = require('./../')

const server = net.createServer((connection) => {

    connection.on('end', () => {

        console.log('Client disconnected');
    })

    connection.on('listening', (data) => {

        console.log('Listening...', data);
    })

})


server.listen(9020, () => {
    console.log('Server listening on ', 9020);
    let streamConfig = {
        redisUrl: 'redis://localhost:6379',
        streams: [{
            name: 'test-transaction_events',
            partitions: 1

        }]
    };

    new Invoker(streamConfig);
    //let i = new Invoker();

    //let i = new Invoker(streamConfig);
    //i.fetch();
    //
    //i.on('message', (data) => {
    //    console.log("+++++++++++++++++++++++++++++++++")
    //    console.log("+++++++++++++++++++++++++++++++++")
    //    console.log(data);
    //    console.log("+++++++++++++++++++++++++++++++++")
    //    console.log("+++++++++++++++++++++++++++++++++")
    //})
})


