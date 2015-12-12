'use strict';

const streamConfig = {
    development: {
        streams: [{
            name: 'development-transaction_events-1',
            partitions: 1
        },{
            name: 'test-transaction_events',
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