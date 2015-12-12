const Aws   = require('aws-sdk');

Aws.config.update({accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    access_secret_access_key: process.env.AWS_SECRET_ACCESS_KEY,
    aws_region: process.env.AWS_REGION
})


module.exports = Aws

