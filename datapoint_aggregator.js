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
        datapoint.measured_date = new Date(datapoint.measured_at.N*1000);

        updateAggregate('month', datapoint, context);

        console.log("Record Loop Complete");
    });
};

function updateAggregate(span, datapoint, context) {
  var record_key = {
    "hive_id_span": { S: datapoint.hive_id.N + span[0] },
    "beepoch_id": { N: getBeepoch(span, datapoint).toString() }
  };

  dynamodb_client.getItem(
    {
      TableName: 'beta_hivebot_aggregates',
      Key: record_key
    },
    function(err, data) {
      if (err) {
        console.log("Aggregator Get Error: %j", err.stack);
      } else {
        item = data.Item || {};
        dynamodb_client.updateItem(
          updateItemParams(datapoint, record_key),
          function(err, data) {
            if (err) {
              console.log("Aggregator Update Error: %j", err.stack);
            } else {
              updateNextAggregateSpan(span, datapoint, context);
            }  
          }
        );
      }
    }
  );
}

function updateItemParams(datapoint, record_key) {
  return {
    TableName: 'beta_hivebot_aggregates',
    Key: record_key,
    UpdateExpression: 'SET first_measurement_at = :first_measurement_at, \
                           last_measurement_at = :last_measurement_at, \
                           max_outside_temp = :max_outside_temp, \
                           min_outside_temp = :min_outside_temp \
                       ADD datapoints_count :one, \
                           total_of_all_outside_temps :outside_temp',
    ExpressionAttributeValues: {
      ":first_measurement_at": { N: (item.first_measurement_at || datapoint.measured_at).N },
      ":last_measurement_at": { N: datapoint.measured_at.N },
      ":max_outside_temp": { N: Math.max((item.max_outside_temp || { N: -999 }).N, datapoint.outside_temp.N).toString() },
      ":min_outside_temp": { N: Math.min((item.min_outside_temp || { N: 999 }).N, datapoint.outside_temp.N).toString() },
      ":one": { N: '1' },
      ":outside_temp": { N: datapoint.outside_temp.N }
    }
  }
}

function updateNextAggregateSpan(span, datapoint, context) {
  switch(span) {
    case 'month':
      updateAggregate('week', datapoint, context);
      break;
    case 'week':
      updateAggregate('day', datapoint, context);
      break;
    case 'day':
      updateAggregate('hour', datapoint, context);
      break;
    case 'hour':
      context.succeed("context succeed");
      break;
  }
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
    (getBeepochYear(datapoint)*12)+datapoint.measured_date.getMonth();
}

function getBeepochYear(datapoint) {
  return beepoch_memo['year'] |=
    datapoint.measured_date.getFullYear() - 2016; 
}
