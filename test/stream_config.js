'use strict';

const streamConfig = {
    development: {
        redisUrl: 'redis://localhost:6379',
        streams: [{
            name: 'development-transaction_events-2',
            partitions: 1
        }]
    },

    staging: {
        streams: [{
        }]
    },

    test: {
        streams: [{
            name: 'test-transaction_events',
            partitions: 1
        }]
    },

    sandbox: {
        streams: [{
        }]
    },

    production: {
        streams: [{
        }]
    }
}

let environment = process.env.NODE_ENV || "development";
module.exports = streamConfig[environment];