require "dyna_model/version"
require "dyna_model/config"
require "dyna_model/attributes"
require "dyna_model/schema"
require "dyna_model/document"

#require "toy/dynamo/adapter"
#require "toy/dynamo/schema"
#require "toy/dynamo/table"
#require "toy/dynamo/tasks"
#require "toy/dynamo/querying"
#require "toy/dynamo/response"
#require "toy/dynamo/persistence"
# Override 'write_attribute' for hash_key == id
#require "toy/dynamo/attributes"
#require "toy/dynamo/store"
#require "toy/dynamo/extensions/array"
#require "toy/dynamo/extensions/boolean"
#require "toy/dynamo/extensions/date"
#require "toy/dynamo/extensions/hash"
#require "toy/dynamo/extensions/set"
#require "toy/dynamo/extensions/time"
#require "toy/dynamo/extensions/symbol"
#require "toy/dynamo/extensions/float"

module DynaModel
  extend self

  def configure
    block_given? ? yield(DynaModel::Config) : DynaModel::Config
  end
  alias :config :configure

  def logger
    DynaModel::Config.logger
  end

end


#module AWS
  #module Record
    #class DynaModel

      #require "dyna_model/attributes"
      #require "dyna_model/finder_methods"
      #require "dyna_model/dynamo_table"
      #require "dyna_model/config"
      ##require 'aws/record/hash_model/attributes'
      ##require 'aws/record/hash_model/finder_methods'
      ##require 'aws/record/hash_model/scope'

      #extend AbstractBase

      #def self.inherited(sub_class)
        ##sub_class.attr_reader :guid #, :hash_key => true, :default_hash_key_attribute => true
      #end

      ##@return [String,nil]
      ##def hash_key
        ##self[self.class.hash_key]
      ##end

      #class << self

        #KEY_TYPE = {
          #:hash => "HASH",
          #:range => "RANGE"
        #}

        #PROJECTION_TYPE = {
          #:keys_only => "KEYS_ONLY",
          #:all => "ALL",
          #:include => "INCLUDE"
        #}

        #def dynamo_table(options={}, &block)
          #if block
            #@dynamo_table_config_block ||= block
          #else
            #@dynamo_table_config_block.call unless @dynamo_table_configged

            ##unless @dynamo_table && @dynamo_table_configged
              ##begin
                ##@dynamo_table = Table.new(table_schema, self.adapter.client, options)
              ##rescue Exception => e
                ### Reset table_schema
                ##@local_secondary_indexes = []
                ##raise e
              ##end
              ##unless options[:novalidate]
                ##validate_key_schema if @dynamo_table.schema_loaded_from_dynamo
              ##end
              ##@dynamo_table_configged = true
            ##end
            #@dynamo_table
          #end
        #end

        #def read_provision(val=nil)
          #if val
            #raise(ArgumentError, "Invalid read provision") unless val.to_i >= 1
            #@dynamo_read_provision = val.to_i
          #else
            #@dynamo_read_provision || AWS::Record::DynaModel::Config.read_provision
          #end
        #end

        #def write_provision(val=nil)
          #if val
            #raise(ArgumentError, "Invalid write provision") unless val.to_i >= 1
            #@dynamo_write_provision = val.to_i
          #else
            #@dynamo_write_provision || AWS::Record::DynaModel::Config.write_provision
          #end
        #end

        #def hash_key(hash_key_key=nil)
          #if hash_key_key
            #hash_key_attribute = self.attributes[hash_key_key.to_s]
            #raise(ArgumentError, "Could not find attribute definition for hash_key #{hash_key_key}") unless hash_key_attribute
            #raise(ArgumentError, "Invalid attribute type for hash_key") unless [AWS::Record::Attributes::StringAttr, AWS::Record::Attributes::IntegerAttr, AWS::Record::Attributes::FloatAttr].include?(hash_key_attribute.class)
            #@dynamo_hash_key = {
              #attribute_name: hash_key_attribute.name,
              #key_type: KEY_TYPE[:hash]
            #}
          #else
            #@dynamo_hash_key
          #end
        #end

        #def range_key(range_key_key=nil)
          #if range_key_key
            #range_key_attribute = self.attributes[range_key_key.to_s]
            #raise(ArgumentError, "Could not find attribute definition for range_key #{range_key_key}") unless range_key_attribute
            #raise(ArgumentError, "Invalid attribute type for range_key") unless [AWS::Record::Attributes::StringAttr, AWS::Record::Attributes::IntegerAttr, AWS::Record::Attributes::FloatAttr].include?(range_key_attribute.class)

            #validates_presence_of range_key_attribute.name.to_sym

            #@dynamo_range_key = {
              #attribute_name: range_key_attribute.name,
              #key_type: KEY_TYPE[:range]
            #}
          #else
            #@dynamo_range_key
          #end
        #end

        #private

        ##def dynamo_db_table_name shard_name = nil
          ##"#{Record.table_prefix}#{self.shard_name(shard_name)}"
        ##end

        ##def dynamo_db
          ##AWS::DynamoDB.new
        ##end

        #def add_attribute(attribute)
          #super(attribute)
        #end
        
      #end

      #private

      #def populate_id
        ##hash_key = self.class.hash_key_attribute
        ##if hash_key.options[:default_hash_key_attribute]
          ##self[hash_key.name] = UUIDTools::UUID.random_create.to_s.downcase
        ##end
      #end

    #end

  #end
#end
