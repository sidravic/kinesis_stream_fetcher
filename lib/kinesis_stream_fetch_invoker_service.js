'use strict';

let constants     = require('./../config/constants.js');

const util         = require('util');
const EventEmitter = require('events')
const _            = require('lodash');
const childProcess = require('child_process');
const path         = require('path');
const Redis        = require('./redis.js');

let internals = {};
internals.streamProcesses = [];
internals.streamConfigMissingError = function streamConfigMissingError(){
    throw new Error('StreamConfig must be an Object and cannot be empty. \n ' +
        '            Expected Parameters are: \n' +
        '            { redisUrl: _redisUrl_\n' +
        '              streams: [\n' +
        '                   name: _some_stream_name_,\n' +
        '                   partitions: _partitions_for_stream_\n' +
                        '] \n' +
        '              }');


};

internals.redisUrlMissingError = function redisUrlMissingError(){
    throw new Error('redisUrl is invalid');
}

class KinesisStreamFetchInvoker extends EventEmitter{

    constructor(streamConfig){
        super();

        if (!(_.isPlainObject(streamConfig) || _.isEmpty(streamConfig))) {
            return internals.streamConfigMissingError();
        }

        this.streamConfig = streamConfig;

        if(_.isEmpty(_.trim(streamConfig.redisUrl)))
            return internals.redisUrlMissingError();

        this.redisClient  = new Redis(streamConfig.redisUrl);
    };

    setupExitHandlers(child, streamName){

        child.on('exit', function onExit(){

            let processObject = _.find(internals.streamProcesses, (streamObject)  => {
                return  (streamObject.streamName == streamName)
            });

            _.pull(internals.streamProcesses, processObject);
            this.launchStreamFetcher(streamName, processObject.partitions);
        })
    }

    setupMessageListeners(child){

        child.on('message', function onMessage(data){

            this.saveLastReadState(data.payload.streamName, data.payload.shardId, data.payload.lastSequenceNumber, (err, reply) => {

                if(err) {
                    throw err;
                }else{
                    console.log(reply);
                }
            });
            this.emit('message', data);
        });
    };

    saveLastReadState(streamName, shardId, lastReadSequenceNumber, cb){
        let key    = streamName
        let value  = JSON.stringify({shardId: shardId, lastReadSequenceNumber: lastReadSequenceNumber});
        this.redisClient.set(key, value, cb);
    };

    findLastReadState(streamName, cb){
        let key = streamName

        this.redisClient.get(key, function onGet(err, reply){

            if(err){
                return cb(err, null);
            }else{
                if (reply == null) {
                    return cb(null, streamName, null, null);
                }else{
                    let response               = JSON.parse(reply);
                    let shardId                = response.shardId;
                    let lastReadSequenceNumber = response.lastReadSequenceNumber;
                    return cb(null, streamName, shardId, lastReadSequenceNumber);
                }

            }
        });
    };

    launchStreamFetcher(streamName, partitions, shardId, lastReadSequenceNumber){

        let filePath              = (path.resolve(__dirname) + '/fetcher_service.js');
        let child                 = childProcess.fork(filePath);
        let processObject         = {
            streamName: streamName,
            streamProcess: child,
            partitions: partitions
        };

        internals.streamProcesses.push(processObject);
        this.setupExitHandlers(child, streamName);

        let payload = {
            streamName: processObject.streamName,
            partitions: processObject.partitions
        };

        _.merge(payload, {shardId: shardId, lastReadSequenceNumber: lastReadSequenceNumber})

        child.send({
            command: constants.init,
            payload: payload
        });

        this.setupMessageListeners(child);
    };

    fetch() {

        let self = this;

        _.map(this.streamConfig.streams, function eachStream(stream){

            self.findLastReadState(stream.name, (err, streamName, shardId, lastReadSequenceNumber) => {

                if(err){
                    console.log(err);
                    self.launchStreamFetcher(stream.name, stream.partitions, null, null);
                }else{
                    self.launchStreamFetcher(streamName, 1, shardId, lastReadSequenceNumber);
                }
            })
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