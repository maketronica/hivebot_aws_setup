console.log('Loading event');

//var doc = require('dynamodb-doc');
//var dynamo = new doc.DynamoDB();

var AWS = require("aws-sdk");
var dynamo = new AWS.DynamoDB({apiVersion: '2012-08-10'}); 

exports.handler = function(event, context) {
  params = {
    TableName: 'foos',
    ReturnConsumedCapacity: 'TOTAL',
    KeyConditionExpression: 'foo_id = :foo_id',
    ExpressionAttributeValues:
      { ":foo_id": { N: "1" }}
  }
  dynamo.query(params, function(err, data) {
    if (err) {
      console.log(err, err.stack);
      context.fail(new Error('Query Failure: ' + err + ' / ' + err.stack)) 
    } else {
      context.done(null, data);  // SUCCESS with message
    }
  });
};
