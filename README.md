# DynaModel

AWS DynamoDB ORM for Rails based on AWS::Record in the aws-sdk gem. Still a work in progress but very functional.

## Installation
```
gem 'dyna_model'
```

## Sample Model
```
class Dude

  include DynaModel::Document
  
  string_attr :hashy
  integer_attr :ranger, default_value: 2
  string_attr :name, default_value: lambda { "dude" }
  boolean_attr :is_dude
  datetime_attr :born
  serialized_attr :cereal
  timestamps

  hash_key :hashy
  range_key :ranger

  set_shard_name "usery"

  local_secondary_index :name
  global_secondary_index(:name_index, { hash_key: :name, projection: [:name] })

  read_provision 4
  write_provision 4
  guid_delimiter "!"

  validates_presence_of :name

end
```

## Sample Methods
```
# Read a single object by Hash and (optionally) Range keys
Dude.read

# Query by Hash and (optionally) Range keys (compatible with Local and Global Secondary Indexes)
Dude.read_range

# Batch read
Dude.read_multiple

# Read by guid (helper for hash + guid_delimiter + range)
Dude.read_guid

# Get count of query
Dude.count_range

# Table scan with more complex filters
Dude.scan

# Create Table
Dude.create_table

# Delete Table
Dude.delete_table

# Rake tasks
rake ddb:create CLASS=Dude
rake ddb:create CLASS=all
rake ddb:destroy CLASS=Dude
rake ddb:destroy CLASS=all
```

# AWS::Record
* http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/Record.html
* https://github.com/aws/aws-sdk-ruby/blob/master/lib/aws/record/abstract_base.rb
