'use strict';
const _ = require('lodash');

let isCommandValid = function(command){

    return (_.isString(command) && !(_.isEmpty(_.trim(command))))
};

let isPayloadValid = function(payload){

    return ((_.isPlainObject(payload) && !(_.isEmpty(payload))));
};

let messageBuilder = {
    build: function build(command, payload) {
        if ((isCommandValid(command)) && (isPayloadValid(payload))) {
            return {
                command: command,
                payload: payload
            };
        }
    }
}
module.exports = messageBuilder;