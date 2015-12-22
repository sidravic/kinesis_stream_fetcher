'use strict';
const childProcess = require('child_process');
let path           = require('path');
let messageBuilder = require('./message_builder.js')
let constants      = require('./../config/constants.js');

class PartitionsFetcher{

    constructor(streamName){
        this.streamName = streamName;
        this.partitionsFetcherProcess = null;
    }

    init(cb){
        let filePath                  = (path.resolve(__dirname) + '/fetcher_service.js');
        let child                     = childProcess.fork(filePath);
        this.partitionsFetcherProcess = child;
        let payload                   = { streamName: this.streamName };
        let message                   = messageBuilder.build(constants.shardInfo, payload);
        child.send(message);
        this.listenOnResponse(cb)
    }

    listenOnResponse(cb){

        let self = this;
        this.partitionsFetcherProcess.on('message', function onPartitionResponse(data) {

            self.killChildProcess();
            if (cb) {
                console.log(data.payload.shardIdsAndSequenceNumbers[0]);
                return cb(data);
            }else{
                console.log(data.payload);
            }

        })
    }

    killChildProcess(){
        this.partitionsFetcherProcess.removeAllListeners();
        this.partitionsFetcherProcess.kill();
    }
}

module.exports = PartitionsFetcher;
