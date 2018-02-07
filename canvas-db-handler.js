'use strict';


//noinspection JSUnresolvedVariable
module.exports.CanvasDbHandler = (event, context, callback) => {

  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Canvas DB Handler Log',
      input: event,
      elasticUrl : process.env.elasticURL || ''
    })
  };


  callback(null, response);
  console.log(response);

};

/*

 2018-01-25T06:27:53.694Z	051a74cf-ee26-40c4-8cc9-8e5860a915f5	this thing got:
 {
 "statusCode": 200,
 "body": "{\"message\":\"Go Serverless v1.0! Your function executed successfully!\",\"input\":{\"Records\":[{\"eventID\":\"4732ffe17aad1d4b51b08227d6194442\",\"eventName\":\"MODIFY\",\"eventVersion\":\"1.1\",\"eventSource\":\"aws:dynamodb\",\"awsRegion\":\"us-west-1\",\"dynamodb\":{\"ApproximateCreationDateTime\":1516861620,\"Keys\":{\"id\":{\"S\":\"one\"}},\"NewImage\":{\"id\":{\"S\":\"one\"},\"content\":{\"S\":\"{some:\\\"thing 8\\\"}\"}},\"OldImage\":{\"id\":{\"S\":\"one\"},\"content\":{\"S\":\"{some:\\\"thing 7\\\"}\"}},\"SequenceNumber\":\"900000000001972105231\",\"SizeBytes\":61,\"StreamViewType\":\"NEW_AND_OLD_IMAGES\"},\"eventSourceARN\":\"arn:aws:dynamodb:us-west-1:255756032687:table/CanvasData/stream/2018-01-25T04:13:02.789\"}]}}"
 }

 */