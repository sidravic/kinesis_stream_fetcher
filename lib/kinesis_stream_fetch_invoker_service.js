'use strict';

let Aws           = require('./aws');
let StreamConfig  = require('./../config/stream_config.js');
let Fetcher       = require('./fetcher_service.js');
let constants     = require('./../config/constants.js');

const _            = require('lodash');
const async        = require('async');
const childProcess = require('child_process');
const path         = require('path');


let internals = {};
internals.streamProcesses = [];

internals.setupExitHandlers = (child, streamName) => {

    child.on('exit', () => {

        let processObject = _.find(internals.streamProcesses, (streamObject)  => {
            console.log(streamObject.streamName);
            return  (streamObject.streamName == streamName)
        });

        _.pull(internals.streamProcesses, processObject);
        console.log(internals.streamProcesses);

        internals.launchStreamFetcher(streamName, processObject.partitions);
    })
};

internals.setupMessageListeners = (child) => {

    child.on('message', (data) => {
        console.log('------------------------------------')
        console.log(data);
        console.log('------------------------------------')
    });
};

internals.launchStreamFetcher = (streamName, partitions) => {

    let filePath              = (path.resolve(__dirname) + '/fetcher_service.js');
    let child                 = childProcess.fork(filePath);
    let processObject         = {
                                  streamName: streamName,
                                  streamProcess: child,
                                  partitions: partitions
                                };

    internals.streamProcesses.push(processObject);
    internals.setupExitHandlers(child, streamName);

    child.send({
        command: constants.init,
        payload: { streamName: processObject.streamName,
                   partitions: processObject.partitions
                 }
    });

    internals.setupMessageListeners(child);
};

let KinesisStreamFetchInvoker = {
    fetch: () => {

        let self = this;
        console.log(StreamConfig);

        _.map(StreamConfig.streams, (stream) => {
            internals.launchStreamFetcher(stream.name, stream.partitions)
        })
    }
}

process.on('SIGINT', () => {
    console.log("Killing in the name of....");
    _.map(internals.streamProcesses, (processObject) => {

       processObject.streamProcess.removeAllListeners();
       processObject.streamProcess.kill();
    });

    process.exit(0);
})


module.exports = KinesisStreamFetchInvoker;