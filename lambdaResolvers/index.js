const AWS = require('aws-sdk')
const s3 = new AWS.S3()

module.exports.queryS3 = async (event, context) => {
  console.log('Event received to lambda', event)
  const query = {
    Bucket: process.env.S3_BUCKET,
    MaxKeys: 2
  }
  const response = await s3.listObjectsV2(query).promise()
  console.log('Response from S3 Listing', response)
  return response
}
