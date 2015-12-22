'use strict';

const streamConfig = {
    development: {
        redisUrl: 'redis://localhost:6379',
        streams: [{
            name: 'development-transaction_events-1',
            partitions: 1
        },{
            name: 'test-transaction_events',
            partitions: 1
        }]
    },

    staging: {
        redisUrl: 'redis://localhost:6379',
        streams: [{
        }]
    },

    test: {
        redisUrl: 'redis://localhost:6379',
        streams: [{
            name: 'development-transaction_events_2',
            partitions: 1
        }, {
            name: 'test-transaction_events',
            partitions: 1
        }]
    },

    sandbox: {
        redisUrl: 'redis://localhost:6379',
        streams: [{
        }]
    },

    production: {
        redisUrl: 'redis://localhost:6379',
        streams: [{
        }]
    }
}

let environment = process.env.NODE_ENV || "development";
module.exports = streamConfig[environment];