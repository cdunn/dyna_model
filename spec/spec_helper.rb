$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

MODELS = File.join(File.dirname(__FILE__), "app/models")

require 'rspec'
require 'dyna_model'
require 'mocha'
require 'aws-sdk-v1'
require 'aws-sdk'

ENV['AWS_ACCESS_KEY_ID'] ||= 'abcd'
ENV['AWS_SECRET_ACCESS_KEY'] ||= '1234'

aws_config = {
  access_key_id: ENV['AWS_ACCESS_KEY_ID'],
  secret_access_key: ENV['AWS_SECRET_ACCESS_KEY'],
  region: "us-west-2"
}
AWS.config(aws_config)

DynaModel::configure do |config|
  config.endpoint = URI.parse('http://localhost:4567')
  config.read_provision = 5
  config.write_provision = 1
  config.namespace = 'test-'
end

# Requires supporting files with custom matchers and macros, etc,
# in ./support/ and its subdirectories.
Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each {|f| require f}

Dir[ File.join(MODELS, "*.rb") ].sort.each { |file| require file }

I18n.enforce_available_locales = false

RSpec.configure do |config|
  config.mock_with(:mocha)

  config.before(:each) do
    client = Aws::DynamoDB::Client.new(aws_config.merge(endpoint: URI.parse('http://localhost:4567')))
    client.list_tables[:table_names].each do |table|
      if table =~ /^#{DynaModel::Config.namespace}/
        client.delete_table(table_name: table)
      end
    end
  end

  config.after(:suite) do
    client = Aws::DynamoDB::Client.new(aws_config.merge(endpoint: URI.parse('http://localhost:4567')))
    client.list_tables[:table_names].each do |table|
      if table =~ /^#{DynaModel::Config.namespace}/
        client.delete_table(table_name: table)
      end
    end
  end
end
