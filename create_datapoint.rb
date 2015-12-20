require 'aws-sdk'
require 'yaml'

config = YAML.load_file('put_data_config.yml')

Aws.config.update({
  region: config['aws']['region'],
  credentials: Aws::Credentials.new(config['aws']['key_id'], config['aws']['secret'])
})

dynamodb = Aws::DynamoDB::Client.new(region: config['aws']['region'])

resp = dynamodb.put_item({
  table_name: 'beta_hivebot_datapoints',
  item: { hive_id: 1, created_at: Time.now.to_i, outside_temp: rand(100) }
})

puts resp.inspect
