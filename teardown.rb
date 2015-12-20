require 'aws-sdk'
require 'yaml'

config = YAML.load_file('teardown_config.yml')

Aws.config.update({
  region: config['aws']['region'],
  credentials: Aws::Credentials.new(config['aws']['key_id'], config['aws']['secret'])
})

dynamodb = Aws::DynamoDB::Client.new(region: config['aws']['region'])
begin
  dynamodb.delete_table({ table_name: 'beta_hivebot_datapoints' })
rescue
end

iam_client = Aws::IAM::Client.new(region: 'us-west-2')

response = iam_client.list_policies(scope: 'All')
invoke_policy = response.policies.detect {|p| p.policy_name == 'BetaAPIGatewayLambdaInvokePolicy' }
if invoke_policy
  begin
    iam_client.detach_role_policy(
      role_name: 'BetaAPIGatewayLambdaInvokeRole',
      policy_arn: invoke_policy.arn
    )
  rescue
  end
  iam_client.delete_policy(policy_arn: invoke_policy.arn)
end

exec_policy = response.policies.detect {|p| p.policy_name == 'BetaAPIGatewayLambdaExecPolicy' }
if exec_policy
  begin
    iam_client.detach_role_policy(
      role_name: 'BetaManualLambdaExecRole',
      policy_arn: exec_policy.arn
    )
  rescue
  end
  iam_client.delete_policy(policy_arn: exec_policy.arn)
end

begin
  iam_client.delete_role(
    role_name: 'BetaAPIGatewayLambdaInvokeRole'
  )
rescue
end
begin
  iam_client.delete_role(
    role_name: 'BetaAPIGatewayLambdaExecRole'
  )
rescue
end


lambda_client = Aws::Lambda::Client.new(region: config['aws']['region'])

begin
  lambda_client.delete_function(
    function_name: 'BetaHelloWorld'
  )
rescue
end

begin
  lambda_client.delete_function(
    function_name: 'BetaHivebotDatapointAggregator'
  )
rescue
end


api_client = Aws::APIGateway::Client.new(region: config['aws']['region'])

resp = api_client.get_rest_apis()

rest_api = resp.items.detect {|i| i.name == 'BetaBeehiveRestApiGET' }
if rest_api
  api_client.delete_rest_api(
    rest_api_id: rest_api.id
  )
end
