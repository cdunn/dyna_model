# DynaModel

AWS DynamoDB ORM for Rails based on AWS::Record in the aws-sdk gem. Still a work in progress but very functional.

## Installation
```
gem 'dyna_model'
```

## Supports
* Range Querying
* Scans
* Local Secondary Indexes
* Global Secondary Indexes
* Query Filtering

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
  
  before_create :do_something
  before_validation on: :create do
    do_something
  end

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
rake ddb:create CLASS=all
rake ddb:destroy CLASS=all
rake ddb:resize CLASS=all
```

## Elasticsearch::Model compatible adapter
```
require 'dyna_model/adapters/elasticsearch/dyna_model_adapter'
class Item
  include DynaModel::Document
  include Elasticsearch::Model
  include Elasticsearch::Model::Callbacks
end
```

## CarrierWave compatible adapter
```
require "dyna_model/adapters/carrierwave/dyna_model"
class Item
  include DynaModel::Document
  mount_uploader :favicon, FaviconUploader
end
```

## S3 Backup
Persist DynaModel records for a particular model to S3 for extra backup. Intended for incremental backups of important records and not intended for low value records or models with high frequency writes.
```
class Item
  include DynaModel::Document
  include DynaModel::Extensions::S3Backup
  
  dyna_model_s3_backup bucket: "dyna_model_backups", prefix: "items"
  
  # dyna_model_s3_backup bucket: "dyna_model_backups", prefix: "items", after_save: lambda { |item|
  #   Item.delay.dyna_model_s3_backup_object(item.guid) # sidekiq write
  # }
end
```


## AWS::Record
* http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/Record.html
* https://github.com/aws/aws-sdk-ruby/blob/master/lib/aws/record/abstract_base.rb
