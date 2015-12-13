'use strict';

const redis = require('redis');

class Redis{
    constructor(connectionUrl){
        this.client = redis.createClient(connectionUrl)
    };

    set(key, value, cb){
        this.client.set(key, value, cb);
    }

    get(key, cb){
        this.client.get(key, cb);
    }
}


module.exports = Redis;