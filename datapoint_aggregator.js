console.log('Loading function');

var AWS = require("aws-sdk");
var dynamodb_client = new AWS.DynamoDB({apiVersion: '2012-08-10'}); 

var beepoch = new Date(2016,0,1);
var beepoch_memo = {};

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));
    event.Records.forEach(function(record) {
        beepoch_memo = {};
        console.log(record.eventID);
        console.log(record.eventName);
        console.log('DynamoDB Record: %j', record.dynamodb);
        
        var datapoint = record.dynamodb.NewImage;

        updateHourlyAggregate(datapoint, context);

        console.log("Record Loop Complete");
    });
};

function updateHourlyAggregate(datapoint, context) {
  var record_key = {
    "hive_id_span": { S: datapoint.hive_id.N + 'h' },
    "beepoch_hour": { N: getBeepoch('hour', datapoint).toString() }
  };

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

function getBeepoch(span, datapoint) {
  switch(span) {
    case 'second':
      return getBeepochSecond(datapoint);
    case 'hour':
      return getBeepochHour(datapoint);
    case 'day':
      return getBeepochDay(datapoint);
    case 'week':
      return getBeepochWeek(datapoint);
    case 'month':
      return getBeepochMonth(datapoint);
    case 'year':
      return getBeepochYear(datapoint);
  }
}

function getBeepochSecond(datapoint) {
  return beepoch_memo['second'] |= datapoint.measured_at.N - beepoch.getTime()/1000;
}

function getBeepochHour(datapoint) {
  return beepoch_memo['hour'] |= Math.floor(getBeepochSecond(datapoint)/3600);
}

function getBeepochDay(datapoint) {
  return beepoch_memo['day'] |= Math.floor(getBeepochHour(datapoint)/24);
}

function getBeepochWeek(datapoint) {
  return beepoch_memo['week'] |= Math.floor(getBeepochDay(datapoint)/7);
}

function getBeepochMonth(datapoint) {
  return beepoch_memo['month'] |=
    (getBeepochYear(datapoint)*12)+getBeepochMeasurementTime(datapoint).getMonth();
}

function getBeepochYear(datapoint) {
  return beepoch_memo['year'] |=
    getBeepochMeasurementTime(datapoint).getFullYear() - 2016; 
}

function getBeepochMeasurementTime(datapoint) {
  return beepoch_memo['measurement_time'] |= new Date(datapoint.measured_at.N*1000);
}
