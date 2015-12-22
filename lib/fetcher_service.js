'use strict';

let Aws          = require('./aws');
let constants    = require('./../config/constants.js');

const _              = require('lodash');
const async          = require('async')
const Kinesis        = new Aws.Kinesis({apiVersion: '2013-12-02'});
const messageBuilder = require('./message_builder')

let getShardsAndSequenceNumbers = function getShardsAndSequenceNumbers(payload, cb){

    let streamName = payload.streamName;
    getShardIds(streamName, function onShardInfo(err, shardIds, startingSequenceNumbers){

        let zippedShardIdAndSequenceNumber = _.zip(shardIds, startingSequenceNumbers);
        let getShardIdsPayload             = {streamName: streamName,
                                              shardIdsAndSequenceNumbers: zippedShardIdAndSequenceNumber};
        let message                        = messageBuilder.build(constants.shardInfoResponse, getShardIdsPayload);
        return cb(message);
    })
}

let getShardIds = (streamName, cb) => {

    let describeStreamParams = {
        StreamName: streamName,
        Limit: 10
    };

    Kinesis.describeStream(describeStreamParams, (err, data) => {

        if(err) {
            return cb(err, null);
        }
        else {
            let shardIds                = _.collect(data.StreamDescription.Shards, (shardPayload) => { return shardPayload.ShardId} );
            let startingSequenceNumbers = _.collect(data.StreamDescription.Shards, (shardPayload) => {return shardPayload.SequenceNumberRange.StartingSequenceNumber})
            return cb(null, shardIds, startingSequenceNumbers);
        }

    })
};

let getShardIterator = (streamName, shardId, startingSequenceNumber, cb) => {

    let shardIteratorType = 'AT_SEQUENCE_NUMBER';

    let shardIteratorParams = {
        ShardId: shardId,
        ShardIteratorType: shardIteratorType, /* required */
        StreamName: streamName,
        StartingSequenceNumber: startingSequenceNumber
    };

    Kinesis.getShardIterator(shardIteratorParams, (err, data) => {

        if(err) {
            return cb(err, null)
        }
        else{
            return cb(null, data.ShardIterator);
        }
    })
};

let getRecords = (shardIterator, cb) => {

    let getRecordParams = {
        ShardIterator: shardIterator,
        Limit: 5
    };

    Kinesis.getRecords(getRecordParams, (err, data) => {

        if(err){
            return cb(err, null);
        }
        else{
            return cb(null, data);
        }
    })
}

let sendDataToParent = (records, streamName, shardId, lastReadSequenceNumber) => {

    if (records.length > 0){
        let message = {
            dataCount: records.length,
            lastSequenceNumber: lastReadSequenceNumber,
            records: records,
            streamName: streamName,
            shardId: shardId,

        };

        process.send({
            command: constants.data,
            payload: message
        });
    }
};


class FetcherService{
    constructor(streamName, partitions, shardId, lastReadSequenceNumber){
        this.streamName = streamName;
        this.partitions = partitions;
        this.shardId    = shardId;
        this.lastReadSequenceNumber = lastReadSequenceNumber;
    };

    init(){
        let self = this;

        if ((this.shardId) && (this.lastReadSequenceNumber)){
            getShardIterator(this.streamName, this.shardId, this.lastReadSequenceNumber, (err, shardIterator) => {

                console.log("Fetching From last fetched point");
                if(err) {
                    throw err;
                }else{
                    this.loopAndGetRecords(shardIterator)
                }

            })

        }else{
            this.fetchFromBeginningOfStream((err, shardIterator) => {


                console.log("Fetching from the beginning");
                if(err) {
                    throw err;
                }else{
                    this.loopAndGetRecords(shardIterator);

                }
            })
        }
    };

    loopAndGetRecords(shardIterator){
        let self     = this;
        let iterator = shardIterator;

        async.forever((next) => {

            getRecords(iterator, (err, data) => {

                if(err){
                    console.log(err);
                    next(err);
                }
                else{
                    iterator                    = data.NextShardIterator;
                    let records                 = data.Records;
                    self.lastReadSequenceNumber = ((records.length > 0) ? records[records.length - 1].SequenceNumber : null);
                    sendDataToParent(records, this.streamName, this.shardId, this.lastReadSequenceNumber);

                    setTimeout(function(){
                        next();
                    }, 1000);

                }
            })
        })
    };

    fetchFromBeginningOfStream(cb){
        let self = this;
        let streamName = this.streamName;

        async.waterfall([
            function(callback){

                getShardIds(streamName, (err, shardIds, startingSequenceNumbers) => {

                    return callback(err, shardIds, startingSequenceNumbers)
                })
            },
            function(shardIds, startingSequenceNumbers, callback){

                self.shardId = shardIds[0];
                getShardIterator(streamName, shardIds[0], startingSequenceNumbers[0], (err, data) => {

                   return callback(err, data)
                })
            }
        ], (err, shardIterator) => {

            if(err){
                return cb(err, null);
            }
            else{
                return cb(null, shardIterator);
            }

        })

    };
}

process.on('message', function onMessage(data){

    if (data.command == constants.init) {
        console.log('Launching...')
        let fetcherService = new FetcherService(data.payload.streamName,
                                                data.payload.partitions,
                                                data.payload.shardId,
                                                data.payload.lastReadSequenceNumber);
        fetcherService.init();
    } else if (data.command == constants.shardInfo) {
        getShardsAndSequenceNumbers(data.payload, function onShardsAndSequenceNumbers(message) {
            process.send(message);
        });
    }else {
        console.log('Invalid Command');
        process.exit(0);
    }

})

module.exports = FetcherService;