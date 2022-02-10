const AWS = require('aws-sdk')
const s3 = new AWS.S3()

module.exports.queryS3 = async (event, context) => {
  console.log('Event received to lambda', event)
  const query = {
    Bucket: process.env.S3_BUCKET,
    MaxKeys: 2
  }
  console.log('Query for S3', query)
  const response1 = await s3.listObjectsV2(query).promise().catch(err => {
    console.log('Err while querying S3', { err, query })
    throw err
  })
  console.log('Response from S3 Listing', JSON.stringify(response1))
  query.ContinuationToken = response1.NextContinuationToken
  const response2 = await s3.listObjectsV2(query).promise()
  console.log('Response 2 ', JSON.stringify(response2))
  delete query.ContinuationToken
  query.StartAfter = response1.Contents[1].Key
  const response3 = await s3.listObjectsV2(query).promise()
  console.log('REsponse 3', JSON.stringify({ response3 }))
  return response3
}
