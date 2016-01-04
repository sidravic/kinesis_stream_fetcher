### Kinesis Stream Fetcher

Kinesis Stream Fetcher is a basic library to fetch messages from a Kinesis 
stream without using the multilang daemon. It works as a simple poller by
launching child processes for each stream that needs to be monitored.

Version 1.0.0 supports multiple partitions and launches a process for each paritition.
The `partitions` config is no longer relevant as the information is not directly fetched 
from Amazon API.

### Install

```
npm install kinesis_stream_fetcher --save
```

### Usage

Before starting development ensure that your AWS configuration parameters
are present in your environment under the following variable.

```
export AWS_ACCESS_KEY_ID="ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="SECRET_ACCESS_KEY"
export AWS_REGION="ap-southeast-1"
export KINESIS_PARTITION_KEY="sweetcoffee"
```

**Update**

Version 1.2.0 includes a backward compatible support for instanceIds. Each instanceId
identifies a deployment uniquely, thus allowing you to run multiple instances of Kinesis
Stream Fetcher. 
 
This was a bug in the earlier versions where it assumed that the library would be used for a
single instance

```
let streamConfig = {
    instanceId: 'instance-1',
    redisUrl: 'redis://localhost:6379',
    streams: [{
        name: 'development-transaction_events-1',
        partitions: 1
    },{
        name: 'test-transaction_events',
        partitions: 1
    }]
};
```

```
const KinesisStreamFetcher = require('kinesis_stream_fetcher');
```
```
let fetcher = new KinesisStreamFetcher(streamConfig);
```

```
fetcher.on('message', (data) => {
  console.log(data);
})
```


### Description

The consumer expects a redis connection to store the last read state of a stream. 
The first attempt is always to begin from the start of the stream. 