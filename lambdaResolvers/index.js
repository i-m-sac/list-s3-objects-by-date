const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const ddbClient = new AWS.DynamoDB.DocumentClient()
const moment = require('moment')
const internal = require('stream')
const uuidByString = require('uuid-by-string')
const _ = require('lodash')

module.exports.queryS3 = async (event, context) => {
  console.log('Event received to lambda', event)
  const query = {
    Bucket: process.env.S3_BUCKET,
    MaxKeys: 2
  }
  const [startAfterKey, interval] = await Promise.all([getStartAfterKey(), getItervalToProcess()])
  if (startAfterKey) query.StartAfter = startAfterKey
  let hasNextPage = true
  let iteration = 1
  let response = {}
  while (hasNextPage && context.getRemainingTimeInMillis() > 60000) {
    response = await s3.listObjectsV2(query).promise()
    console.log('Response ' + iteration++, response)
    if (response?.Contents?.length) {
      hasNextPage = response.IsTruncated
      query.StartAfter = response.Contents[response.Contents.length - 1].Key
      await addS3ContentsToDDb(response.Contents.filter(content => {
        const epochTimestamp = +new Date(content.LastModified)
        return (epochTimestamp > interval.startTime && epochTimestamp < internal.endTime)
      }))
    } else {
      hasNextPage = false
    }
  }
  const ddbPayload = {
    id: '1',
    s3Key: response.Contents[response.Contents.length - 1].Key,
    timestamp: +new Date(response.Contents[response.Contents.length - 1].LastModified)
  }
  await ddbClient.put({
    TableName: 'StateTable',
    Item: ddbPayload
  }).promise().catch(err => {
    console.log('Err in adding to state table', { err, ddbPayload })
  })
  return response
}

const addS3ContentsToDDb = async (items, tableName = 'ProcessedFilesTable') => {
  items = items.map(item => { return { id: uuidByString(item.Key), s3Key: item.Key, timestamp: +new Date(item.LastModified) } })
  const chunks = _.chunks(items, 20)
  for (const chunk of chunks) {
    const payload = {
      RequestItems: {
        [tableName]: []
      }
    }
    chunk.forEach(item => {
      payload.RequestItems[tableName].push({
        PutRequest: {
          Item: item
        }
      })
    })
    // console.log('Payload for ddb', JSON.stringify(payload))
    const response = await ddbClient.batchWrite(payload).promise()
    console.log('ResponseFromDDb Write', response)
  }
  return true
}

const getStartAfterKey = async () => {
  const stateRecord = await getStateItem('1')
  return stateRecord?.s3Key || null
}

const getItervalToProcess = async () => {
  const { startTime, endTime } = process.env
  if (startTime && endTime) {
    return { startTime, endTime }
  } else {
    const stateRecord = await getStateItem('2')
    return stateRecord
  }
}

function getStateItem (id) {
  const query = {
    Key: id,
    TableName: 'StateTable'
  }
  return ddbClient.getItem(query).promise()
}
