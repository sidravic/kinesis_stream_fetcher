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

    let redis = null;

    after(function(){
        redis.client.del('test-transaction_events');
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

            redis = new Redis(streamConfig.redisUrl);
            let lastReadState = { shardId: 'shardId-000000000000',
                                  lastReadSequenceNumber: '49557082144852057537246038354925096155420087440296116226'
                                };
            redis.set(streamConfig.streams[0].name, JSON.stringify(lastReadState));
            let invoker = new KinesisStreamFetchInvoker(streamConfig)

            invoker.findLastReadState(streamConfig.streams[0].name, function lastReadState(err, streamName, shardId, lastReadSequenceNumber){

                expect(lastReadSequenceNumber).to.eql('49557082144852057537246038354925096155420087440296116226');
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

            redis = new Redis(streamConfig.redisUrl);
            let lastReadState = { shardId: 'shardId-000000000000',
                lastReadSequenceNumber: '49557082144852057537246038354925096155420087440296116226'
            };
            redis.set(streamConfig.streams[0].name, JSON.stringify(lastReadState));

            let invoker = new KinesisStreamFetchInvoker(streamConfig)

            let launchStreamInvokerSpy = sinon
                .stub(invoker, 'launchStreamFetcher')
                .returns(true)


            invoker.findLastReadState(streamConfig.streams[0].name, function lastReadState(err, streamName, shardId, lastReadSequenceNumber){


                if(err){
                    console.log(err);
                    invoker.launchStreamFetcher(stream.name, stream.partitions, null, null);
                }else{
                    invoker.launchStreamFetcher(streamName, 1, shardId, lastReadSequenceNumber);
                }

                expect(invoker.launchStreamFetcher).to.have.been.calledWith(streamName, 1, shardId, lastReadSequenceNumber);
                done();
            })
        })
    });
})


