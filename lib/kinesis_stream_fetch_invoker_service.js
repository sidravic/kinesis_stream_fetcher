'use strict';

let constants     = require('./../config/constants.js');

const util         = require('util');
const EventEmitter = require('events')
const _            = require('lodash');
const childProcess = require('child_process');
const path         = require('path');

let internals = {};
internals.streamProcesses = [];

class KinesisStreamFetchInvoker extends EventEmitter{

    constructor(streamConfig){
        super();
        this.streamConfig = streamConfig;
    };

    setupExitHandlers(child, streamName){

        child.on('exit', () => {

            let processObject = _.find(internals.streamProcesses, (streamObject)  => {
                return  (streamObject.streamName == streamName)
            });

            _.pull(internals.streamProcesses, processObject);
            this.launchStreamFetcher(streamName, processObject.partitions);
        })
    }

    setupMessageListeners(child){

        child.on('message', (data) => {
            this.emit('message', data);
        });
    }

    launchStreamFetcher(streamName, partitions){

        let filePath              = (path.resolve(__dirname) + '/fetcher_service.js');
        let child                 = childProcess.fork(filePath);
        let processObject         = {
            streamName: streamName,
            streamProcess: child,
            partitions: partitions
        };

        internals.streamProcesses.push(processObject);
        this.setupExitHandlers(child, streamName);

        child.send({
            command: constants.init,
            payload: { streamName: processObject.streamName,
                partitions: processObject.partitions
            }
        });

        this.setupMessageListeners(child);
    };

    fetch() {

        let self = this;

        _.map(this.streamConfig.streams, (stream) => {
            self.launchStreamFetcher(stream.name, stream.partitions)
        })
    }
}


process.on('SIGINT', () => {
    console.log("Stopping child processes....");
    _.map(internals.streamProcesses, (processObject) => {

       processObject.streamProcess.removeAllListeners();
       processObject.streamProcess.kill();
    });
    console.log("Done.");

    process.exit(0);
})


module.exports = KinesisStreamFetchInvoker;