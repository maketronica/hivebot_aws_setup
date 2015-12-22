console.log('Loading function');

var AWS = require("aws-sdk");
var dynamodb_client = new AWS.DynamoDB({apiVersion: '2012-08-10'}); 

var beepoch = new Date(2016,0,1);

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));
    event.Records.forEach(function(record) {
        console.log(record.eventID);
        console.log(record.eventName);
        console.log('DynamoDB Record: %j', record.dynamodb);
        
        var datapoint = record.dynamodb.NewImage;

        updateHourlyAggregate(datapoint, context);

        console.log("Record Loop Complete");
    });
};

function updateHourlyAggregate(datapoint, context) {
  var measurement_time = new Date(datapoint.measured_at.N*1000);
  var beepoch_second = measurement_time.getTime()/1000 - beepoch.getTime()/1000;
  var beepoch_hour = Math.floor(beepoch_second/3600);
  var beepoch_day = Math.floor(beepoch_hour/24);
  var beepoch_week = Math.floor(beepoch_day/7);

  var beepoch_year = measurement_time.getFullYear() - 2016; 
  var beepoch_month = (beepoch_year*12)+measurement_time.getMonth();

  var record_key = {
    "hive_id_span": { S: datapoint.hive_id.N + 'h' },
    "beepoch_hour": { N: beepoch_hour.toString() }
  };

  console.log("Initializing Aggregate: " + datapoint.hive_id.N + "/" + beepoch_hour);

  dynamodb_client.getItem(
    {
      TableName: 'beta_hivebot_aggregates',
      Key: record_key
    },
    function(err, data) {
      if (err) {
        console.log("Aggregator Init Error: %j", err.stack);
      } else {
        item = data.Item || {};
        dynamodb_client.updateItem(
          {
            TableName: 'beta_hivebot_aggregates',
            Key: record_key,
            UpdateExpression: 'SET last_updated_at = :last_datapoint_measured_at, \
                                   max_outside_temp = :max_outside_temp, \
                                   min_outside_temp = :min_outside_temp \
                               ADD datapoints_count :one, \
                                   total_of_all_outside_temps :outside_temp',
            ExpressionAttributeValues: {
              ":last_datapoint_measured_at": { N: datapoint.measured_at.N },
              ":max_outside_temp": { N: Math.max((item.max_outside_temp || { N: -999 }).N, datapoint.outside_temp.N).toString() },
              ":min_outside_temp": { N: Math.min((item.min_outside_temp || { N: 999 }).N, datapoint.outside_temp.N).toString() },
              ":one": { N: '1' },
              ":outside_temp": { N: datapoint.outside_temp.N }
            }
          },
          function(err, data) {
            if (err) {
              console.log("Aggregator Init Error: %j", err.stack);
            } else {
              console.log("Aggregator Init Succ: %j", data);
              context.succeed("context succeed");
            }  
          }
        );
      }
    }
  );
}
