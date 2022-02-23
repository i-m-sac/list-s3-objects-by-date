const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const ddbClient = new AWS.DynamoDB.DocumentClient()
const moment = require('moment')
const uuidByString = require('uuid-by-string')
const _ = require('lodash')
var lambda = new AWS.Lambda();

module.exports.queryS3 = async (event, context) => {
  console.log('Event received to lambda', event)
  const query = {
    Bucket: process.env.S3_BUCKET,
    MaxKeys: 2
  }
  const [startAfterKey] = await Promise.all([getStartAfterKey()])
  let interval = {}
  if (startAfterKey) query.StartAfter = startAfterKey
  let hasNextPage = true
  let iteration = 1
  let response = {}
  while (hasNextPage && context.getRemainingTimeInMillis() > 60000) {
    response = await s3.listObjectsV2(query).promise()
    console.log('Response ' + iteration++, response)
    if (response?.Contents?.length) {
      hasNextPage = response.IsTruncated
      response.Contents = response.Contents.filter(content => {
        const epochTimestamp = +new Date(content.LastModified)
        if (interval && interval.startTime) {
          return (epochTimestamp > interval.startTime && epochTimestamp < interval.endTime)
        } else {
          return true
        }
      })
      const itemsMetadata = await getS3ItemsMetadata(response.Contents)
      query.StartAfter = response.Contents[response.Contents.length - 1].Key
      await addS3ContentsToDDb(itemsMetadata)
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
  const chunks = _.chunk(items, 20)
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

async function getStateItem (id) {
  const query = {
    Key: { id },
    TableName: 'StateTable'
  }
  return ddbClient.get(query).promise().catch(err => {
    console.log('Err in get Item ', JSON.stringify({ query, err }))
  })
}

async function invokeLambda () {
  if (process.env.invokeLambas !== 'allowed') { return false }
  const params = {
    FunctionName: `${process.env.stage}-queryS3`, // the lambda function we are going to invoke
    InvocationType: 'Event',
    Payload: ''
  };
  return lambda.invoke(params).promise()  
}

async function getS3ItemsMetadata (items) {
  const chunks = _.chunk(items, 20)
  const metadataArr = []
  for (const chunk of chunks) {
    const promises = []
    chunk.forEach(item => {
      promises.push(getS3ItemMetadata(item.Key))
    })
    const response = await Promise.all(promises).catch(err => {
      console.log('Err in fetching object metadata', { err })
    })
    metadataArr.push(...response)
  }
  return metadataArr
}

async function getS3ItemMetadata (key) {
  const params = {
    Key: key,
    Bucket: process.env.S3_BUCKET
  }
  const response = await s3.headObject(params).promise().catch(err => {
    console.log('Err while fetching item metadat', { key, err })
    return null
  })
  response.Key = key
  return response
}