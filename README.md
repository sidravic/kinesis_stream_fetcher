### Kinesis Stream Fetcher

Kinesis Stream Fetcher is a basic library to fetch messages from a Kinesis 
stream without using the multilang daemon. It works as a simple poller by
launching child processes for each stream that needs to be monitored.

### Usage

Before starting development ensure that your AWS configuration parameters
are present in your environment under the following variable.

```
export AWS_ACCESS_KEY_ID="ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="SECRET_ACCESS_KEY"
export AWS_REGION="ap-southeast-1"
export KINESIS_PARTITION_KEY="sweetcoffee"
```

```
let streamConfig = {
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
