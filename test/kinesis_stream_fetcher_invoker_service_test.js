'use strict';
const chai                      = require('chai');
const expect                    = require('chai').expect;
const sinon                     = require('sinon');
const sinonChai                 = require('sinon-chai');
const KinesisStreamFetchInvoker = require('./../lib/kinesis_stream_fetch_invoker_service.js');
const Redis                     = require('./../lib/redis.js');
chai.use(sinonChai);


describe('new KinesisStreamFetchInvoker', function(){

    context('when there is no configuration JSON provided', function(){

        it('should throw an error saying streamConfig cannot be empty', function(){
            expect(function testFunc() {
                new KinesisStreamFetchInvoker()
            }).to.throw()
        })
    });

    context('when redis URL is not provided', function(){

        it('should throw an error saying redisUrl should not be empty', function(){
            function testFunc() {
                let streamConfig = {
                    streams: {
                        name: 'test-transaction_events',
                        partitions: 1

                    }
                };
                new KinesisStreamFetchInvoker(streamConfig)
            };

            expect(testFunc).to.throw();
        })
    })


    context('when all streamConfig data is available', function(){

        it('should not throw any errors', function(){

            function testFunc(){
                let streamConfig = {
                    redisUrl: 'redis://localhost:6379',
                    streams: [{
                        name: 'test-transaction_events',
                        partitions: 1

                    }]
                };
                new KinesisStreamFetchInvoker(streamConfig);
            };

            expect(testFunc).not.to.throw();
        })
    })
});


describe('.fetch', function(){

    let redis = new Redis('redis://localhost:6379');

    function flushRedis(cb) {
        redis.client.flushall(cb)
    }

    beforeEach(function(done) {

        flushRedis(function onFlush(err, data){
            done();
        });
    })

    afterEach(function(done){

        flushRedis(function onFlush(err, data){

            done();
        });
    })

    context('when the stream has a last read state', function(){

        it('should fetch the last read state if it exists', function(done){

            let streamConfig = {
                redisUrl: 'redis://localhost:6379',
                streams: [{
                    name: 'test-transaction_events',
                    partitions: 1
                }]
            };

            let lastReadState = { shardId: 'shardId-000000000000',
                lastReadSequenceNumber: '49557082144852057537246038354925096155420087440296116226'
            };

            let key = streamConfig.streams[0].name.toString() + lastReadState.shardId.toString();

            let invoker = new KinesisStreamFetchInvoker(streamConfig)

            sinon.stub(redis, 'get').yields(null, JSON.stringify({
                    streamName: streamConfig.streams[0].name,
                    shardId:lastReadState.shardId,
                    lastReadSequenceNumber: lastReadState.lastReadSequenceNumber }))

            invoker.redisClient = redis;
            invoker.findLastReadState(streamConfig.streams[0].name, lastReadState.shardId, function lastReadState(err, streamName, shardId, lastReadSequenceNumber){

                expect(lastReadSequenceNumber).to.eql('49557082144852057537246038354925096155420087440296116226');
                redis.get.restore();
                done();
            })
        })

        it('should call launchStreamFetcher with the last read state parameters', function(done){

            let streamConfig = {
                redisUrl: 'redis://localhost:6379',
                streams: [{
                    name: 'test-transaction_events',
                    partitions: 1
                }]
            };

            let lastReadState = { shardId: 'shardId-000000000000',
                lastReadSequenceNumber: '49557082144852057537246038354925096155420087440296116226'
            };

            sinon.stub(redis, 'get').yields(null, JSON.stringify({
                streamName: streamConfig.streams[0].name,
                shardId:lastReadState.shardId,
                lastReadSequenceNumber: lastReadState.lastReadSequenceNumber }))

            let invoker = new KinesisStreamFetchInvoker(streamConfig)
            invoker.redisClient = redis;

            sinon
                .stub(invoker, 'launchStreamFetcher')
                .returns(true)

            sinon.stub(invoker, 'findLastReadState').yields(null,
                                                            streamConfig.streams[0].name,
                                                            lastReadState.shardId,
                                                            lastReadState.lastReadSequenceNumber
                                                            )


            invoker.findLastReadState(streamConfig.streams[0].name, lastReadState.shardId, function onLastReadState(err, _streamName, _shardId, lastReadSequenceNumber){

                if(err || (lastReadSequenceNumber == null)){
                    invoker.launchStreamFetcher(_streamName, 1, _shardId, lastReadSequenceNumber);
                }else{
                    invoker.launchStreamFetcher(_streamName, 1, _shardId, lastReadSequenceNumber);
                }

                expect(invoker.launchStreamFetcher).to.have.been.calledWith(streamConfig.streams[0].name, 1,
                                                                            lastReadState.shardId, lastReadState.lastReadSequenceNumber);
                done();
            })

        })
    });
})

describe('.launchStreamFetcher', function(){

    let redis = new Redis('redis://localhost:6379');

    function flushRedis(cb) {
        redis.client.flushall(cb)
    }

    beforeEach(function(done) {

        flushRedis(function onFlush(err, data){
            done();
        });
    })

    afterEach(function(done){

        flushRedis(function onFlush(err, data){

            done();
        });
    })
})


