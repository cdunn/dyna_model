$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

MODELS = File.join(File.dirname(__FILE__), "app/models")

require 'rspec'
require 'dyna_model'
require 'mocha'
require 'aws-sdk'

ENV['ACCESS_KEY'] ||= 'abcd'
ENV['SECRET_KEY'] ||= '1234'

aws_config = {
  access_key_id: ENV['ACCESS_KEY'],
  secret_access_key: ENV['SECRET_KEY'],
  dynamo_db_endpoint: 'localhost',
  dynamo_db_port: '4567',
  use_ssl: false
}
AWS.config(aws_config)

DynaModel::configure do |config|
  config.endpoint = 'localhost'
  config.port = 4567
  config.use_ssl = false
  config.read_provision = 5
  config.write_provision = 1
  config.namespace = 'test-'
end

# Requires supporting files with custom matchers and macros, etc,
# in ./support/ and its subdirectories.
Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each {|f| require f}

Dir[ File.join(MODELS, "*.rb") ].sort.each { |file| require file }

RSpec.configure do |config|
  config.mock_with(:mocha)

  config.before(:each) do
    client = AWS::DynamoDB::Client.new(aws_config.merge(api_version: '2012-08-10'))
    client.list_tables[:table_names].each do |table|
      if table =~ /^#{DynaModel::Config.namespace}/
        client.delete_table(table_name: table)
      end
    end
  end

  config.after(:suite) do
    client = AWS::DynamoDB::Client.new(aws_config.merge(api_version: '2012-08-10'))
    client.list_tables[:table_names].each do |table|
      if table =~ /^#{DynaModel::Config.namespace}/
        client.delete_table(table_name: table)
      end
    end
  end
end
