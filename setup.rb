require 'aws-sdk'
require 'yaml'
require 'zip'

config = YAML.load_file('config.yml')

Aws.config.update({
  region: config['aws']['region'],
  credentials: Aws::Credentials.new(config['aws']['key_id'], config['aws']['secret'])
})

dynamodb = Aws::DynamoDB::Client.new(region: config['aws']['region'])

datapoints_table = dynamodb.create_table({
  table_name: 'beta_hivebot_datapoints',
  attribute_definitions: [
    { attribute_name: 'hive_id', attribute_type: 'N' },
    { attribute_name: 'measured_at', attribute_type: 'N' }
  ],
  key_schema: [
    { attribute_name: 'hive_id', key_type: 'HASH' },
    { attribute_name: 'measured_at', key_type: 'RANGE' }
  ],
  provisioned_throughput: { read_capacity_units: 1, write_capacity_units: 1, },
  stream_specification: {
    stream_enabled: true,
    stream_view_type: "NEW_AND_OLD_IMAGES"
  }
}).table_description

hourly_aggregates_table = dynamodb.create_table({
  table_name: 'beta_hivebot_aggregates',
  attribute_definitions: [
    { attribute_name: 'hive_id_span', attribute_type: 'S' },
    { attribute_name: 'beepoch_id', attribute_type: 'N' },
  ],
  key_schema: [
    { attribute_name: 'hive_id_span', key_type: 'HASH' },
    { attribute_name: 'beepoch_id', key_type: 'RANGE' }
  ],
  provisioned_throughput: { read_capacity_units: 1, write_capacity_units: 1, },
  stream_specification: {
    stream_enabled: true,
    stream_view_type: "NEW_AND_OLD_IMAGES"
  }
}).table_description

iam_client = Aws::IAM::Client.new(region: 'us-west-2')

lambda_invoke_policy = iam_client.create_policy(
  policy_name: 'BetaAPIGatewayLambdaInvokePolicy',
  policy_document: <<eos
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Resource": [
        "*"
      ],
      "Action": [
        "lambda:InvokeFunction"
      ]
    }
  ]
}
eos
).policy

lambda_exec_policy = iam_client.create_policy(
  policy_name: 'BetaAPIGatewayLambdaExecPolicy',
  policy_document: <<eos
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "logs:*",
        "dynamodb:Query"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:*:*:*",
        "arn:aws:dynamodb:*"
      ]
    }
  ]
}
eos
).policy

lambda_invoke_role = iam_client.create_role(
  role_name: "BetaAPIGatewayLambdaInvokeRole",
  assume_role_policy_document: <<eos
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "apigateway.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
eos
).role

#lambda_exec_role = iam_client.create_role(
#  role_name: "BetaAPIGatewayLambdaExecRole",
#  assume_role_policy_document: <<eos
#{
#  "Version": "2012-10-17",
#  "Statement": [
#    {
#      "Effect": "Allow",
#      "Principal": {
#        "Service": "lambda.amazonaws.com"
#      },
#      "Action": "sts:AssumeRole"
#    }
#  ]
#}
#eos
#).role

lambda_exec_role = iam_client.get_role(
  role_name: 'BetaManualLambdaExecRole'
).role

lambda_dynamo_stream_reader_role = iam_client.get_role(
  role_name: 'LambdaDynamoStreamReader'
).role

iam_client.attach_role_policy(
  role_name: lambda_invoke_role.role_name,
  policy_arn: lambda_invoke_policy.arn
)

iam_client.attach_role_policy(
  role_name: lambda_exec_role.role_name,
  policy_arn: lambda_exec_policy.arn
)

lambda_client = Aws::Lambda::Client.new(region: config['aws']['region'])

aggregator_zip = Zip::OutputStream.write_buffer do |zio|
  zio.put_next_entry('index.js')
  zio.write(File.open('datapoint_aggregator.js', 'rb').read)
end

lambda_zip = Zip::OutputStream.write_buffer do |zio|
  zio.put_next_entry('index.js')
  zio.write(File.open('hello_world.js', 'rb').read)
end

functions = lambda_client.list_functions().functions

if (functions.any? {|function| function.function_name == 'BetaHivebotDatapointAggregator' }) then
  lambda_client.update_function_code({
    function_name: 'BetaHivebotDatapointAggregator',
    zip_file: aggregator_zip.string
  })
else
  sleep 5; #wait for role to propogate
  lambda_client.create_function({
    function_name: 'BetaHivebotDatapointAggregator',
    runtime: 'nodejs',
    handler: 'index.handler',
    role: lambda_dynamo_stream_reader_role.arn,
    code: {
      zip_file: aggregator_zip.string
    }
  })
end

if (functions.any? {|function| function.function_name == 'BetaHelloWorld' }) then
  lambda_client.update_function_code({
    function_name: 'BetaHelloWorld',
    zip_file: lambda_zip.string
  })
else
  sleep 5; #wait for role to propogate
  lambda_client.create_function({
    function_name: 'BetaHelloWorld',
    runtime: 'nodejs',
    handler: 'index.handler',
    role: lambda_exec_role.arn,
    code: {
      zip_file: lambda_zip.string
    }
  })
end

lambda_client.create_event_source_mapping(
  event_source_arn: datapoints_table.latest_stream_arn,
  function_name: 'BetaHivebotDatapointAggregator',
  starting_position: "TRIM_HORIZON"
)

api_client = Aws::APIGateway::Client.new(region: config['aws']['region'])

rest_api = api_client.create_rest_api(
  name: 'BetaBeehiveRestApiGET'
)

resources = api_client.get_resources(
  rest_api_id: rest_api.id,
).items

puts resources.inspect

root_resource = resources.detect {|r| r.path == '/' }

resource = api_client.create_resource(
  rest_api_id: rest_api.id,
  path_part: 'betabeehives',
  parent_id: root_resource.id
)

api_client.put_method(
  rest_api_id: rest_api.id,
  resource_id: resource.id,
  http_method: 'GET',
  authorization_type: ''
)

api_client.put_integration(
  rest_api_id: rest_api.id,
  resource_id: resource.id,
  http_method: 'GET',
  type: 'MOCK' # APIGateway::Client#put_integration doesn't support
               # 'LAMBDA' type, so still need to manually go in to
               # aws_console/api_gateway/betabeehiveRestApiGET/
               #   betabeehives/GET/IntegrationRequest
               # and manually change integration type to
               # "Lambda Function" and set region and function.
)

api_client.put_method_response(
  rest_api_id: rest_api.id,
  resource_id: resource.id,
  http_method: 'GET',
  status_code: '200',
  response_models: {
    'application/json': 'Empty'
  }
)

api_client.create_deployment(
  rest_api_id: rest_api.id,
  stage_name: 'prod'
)
