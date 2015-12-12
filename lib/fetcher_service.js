'use strict';

let Aws          = require('./aws');
let constants    = require('./../config/constants.js');

const _       = require('lodash');
const async   = require('async')
const Kinesis = new Aws.Kinesis({apiVersion: '2013-12-02'});

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
        Limit: 30
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

let sendDataToParent = (records, streamName) => {

    if (records.length > 0){
        let message = {
            dataCount: records.length,
            lastSequenceNumber: records[records.length - 1].SequenceNumber,
            records: records,
            streamName: streamName
        };

        process.send({
            command: constants.data,
            payload: message
        });
    }
}

class FetcherService{
    constructor(streamName, partitions){
        this.streamName = streamName;
        this.partitions = partitions;
    };

    init(){
        let self = this;
        this.fetch((err, shardIterator ) => {

            if(err) {
                throw err;
            }
            else{
                let iterator = shardIterator;

                async.forever((next) => {

                    getRecords(iterator, (err, data) => {

                        if(err){
                            console.log(err);
                            next(err);
                        }
                        else{
                            iterator    = data.NextShardIterator;
                            let records = data.Records;
                            sendDataToParent(records, this.streamName);
                            next();
                        }
                    })
                })
            }
        })
    }

    fetch(cb){
        let self = this;
        let streamName = this.streamName;

        async.waterfall([
            function(callback){

                getShardIds(streamName, (err, shardIds, startingSequenceNumbers) => {

                    return callback(err, shardIds, startingSequenceNumbers)
                })
            },
            function(shardIds, startingSequenceNumbers, callback){

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

process.on('message', (data) => {
    console.log('Launching...')

    if (data.command == constants.init) {
        let fetcherService = new FetcherService(data.payload.streamName, data.partitions)
        fetcherService.init();
    } else {
        console.log('Invalid Command');
    }

})

module.exports = FetcherService;