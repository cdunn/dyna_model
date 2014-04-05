module DynaModel
  module Document

    MAX_ITEM_SIZE = 65_536
    # These delimiters are also reserved characters and should not be used in
    # hash or range keys
    GUID_DELIMITER_PRECEDENCE = ["_", ":", "|", ",", "!", "~", "@", "^"]

    extend ActiveSupport::Concern

    included do
      class_attribute :read_only_attributes, :base_class
      self.base_class = self

      AWS::Record.table_prefix = "#{DynaModel::Config.namespace}#{Rails.application.class.parent_name.to_s.underscore.dasherize}-#{Rails.env}-"

      extend ActiveModel::Translation
      extend ActiveModel::Callbacks
      extend AWS::Record::AbstractBase
      include DynaModel::Persistence
      include DynaModel::Validations

      define_model_callbacks :create, :save, :destroy, :initialize, :update, :validation

      # OVERRIDE
      # https://github.com/aws/aws-sdk-ruby/blob/master/lib/aws/record/abstract_base.rb#L258
      # AWS::Record::AbstractBase for :select attributes
      protected
      def [] attribute_name
        # Warn if using attributes that were not part of the :select (common with GSI/LSI projections)
        #   we do not want to give the impression they are nil
        if (selected_attrs = self.instance_variable_get("@_selected_attributes"))
          raise "Attribute '#{attribute_name}' was not part of the select '#{self.instance_variable_get("@_select")}' (available attributes: #{selected_attrs})" unless selected_attrs.include?(attribute_name)
        end
        super
      end
    end

    include ActiveModel::Conversion
    include ActiveModel::MassAssignmentSecurity if defined?(ActiveModel::MassAssignmentSecurity)
    include ActiveModel::Naming
    include ActiveModel::Observing if defined?(ActiveModel::Observing)
    include ActiveModel::Serializers::JSON
    include ActiveModel::Serializers::Xml
    
    include DynaModel::Attributes
    include DynaModel::Schema
    include DynaModel::Query

    def to_param
      self.dynamo_db_guid
    end

    def dynamo_db_guid
      _guid = [self.dynamo_db_item_key_values[:hash_value]]
      _guid << self.dynamo_db_item_key_values[:range_value] if self.dynamo_db_item_key_values[:range_value]
      _guid.join(self.class.guid_delimiter)
    end

    def dynamo_db_item_key_values
      key_values = { hash_value: self[self.class.hash_key[:attribute_name]] }
      key_values.merge!(range_value: self[self.class.range_key[:attribute_name]]) if self.class.range_key
      key_values
    end

    def all_attributes_loaded?
      self.instance_variable_get("@_select") == :all
    end

    # When only partial attributes were selected (via GSI or projected attributes on an index)
    def load_attributes!
      raise "All attributes already loaded!" if self.instance_variable_get("@_select") == :all
      options = { shard_name: self.shard }
      if self.class.range_key
        obj = self.class.read(dynamo_db_item_key_values[:hash_value], dynamo_db_item_key_values[:range_value], options)
      else
        obj = self.class.read(dynamo_db_item_key_values[:hash_value], options)
      end
      raise "Could not find object" unless obj
      self.instance_variable_set("@_select", :all)
      self.remove_instance_variable("@_selected_attributes")
      self.instance_variable_set("@_data", obj.instance_variable_get("@_data"))
      self
    end

    def touch
      self.send(:touch_timestamps, "updated_at")
    end

    def touch!
      self.touch
      self.save
    end

    module ClassMethods

      def create_table options = {}
        table_name = self.dynamo_db_table_name(options[:shard_name])
        if self.dynamo_db_client.list_tables[:table_names].include?(table_name)
          puts "Table #{table_name} already exists"
          return false
        end
        self.dynamo_db_client.create_table(self.table_schema.merge({
          table_name: table_name
        }))
        while (table_metadata = self.describe_table(options))[:table][:table_status] == "CREATING"
          sleep 1
        end
        table_metadata
      end

      def describe_table(options={})
        self.dynamo_db_client.describe_table(table_name: self.dynamo_db_table_name(options[:shard_name]))
      end

      def delete_table(options={})
        table_name = self.dynamo_db_table_name(options[:shard_name])
        return false unless self.dynamo_db_client.list_tables[:table_names].include?(table_name)
        self.dynamo_db_client.delete_table(table_name: table_name)
        begin
          while (table_metadata = self.describe_table) && table_metadata[:table][:table_status] == "DELETING"
            sleep 1
          end
        rescue AWS::DynamoDB::Errors::ResourceNotFoundException => e
          DynaModel::Config.logger.info "Table deleted"
        end
        true
      end

      def resize_table(options={})
        table_name = self.dynamo_db_table_name(options[:shard_name])
        return false unless self.dynamo_db_client.list_tables[:table_names].include?(table_name)
        self.dynamo_db_client.update_table({
          provisioned_throughput: {
            read_capacity_units: (options[:read_capacity_units] || self.table_schema[:provisioned_throughput][:read_capacity_units]).to_i,
            write_capacity_units: (options[:write_capacity_units] || self.table_schema[:provisioned_throughput][:write_capacity_units]).to_i
          },
          table_name: table_name
        })
        while (table_metadata = self.describe_table) && table_metadata[:table][:table_status] == "UPDATING"
          sleep 1
        end
        DynaModel::Config.logger.info "Table resized to #{table_metadata[:table][:provisioned_throughput]}"
        true
      end

      def dynamo_db_table(shard_name = nil)
        @table_map ||= {}
        @table_map[self.dynamo_db_table_name(shard_name)] ||= Table.new(self)
      end

      def dynamo_db_table_name(shard_name = nil)
        "#{AWS::Record.table_prefix}#{self.shard_name(shard_name)}"
      end

      def dynamo_db_client(config={})
        options = {}
        options[:use_ssl] = DynaModel::Config.use_ssl
        options[:use_ssl] = config[:use_ssl] if config.has_key?(:use_ssl)
        options[:dynamo_db_endpoint] = config[:endpoint] || DynaModel::Config.endpoint
        options[:dynamo_db_port] = config[:port] || DynaModel::Config.port
        options[:api_version] ||= config[:api_version] || '2012-08-10'

        @dynamo_db_client ||= AWS::DynamoDB::Client.new(options)
      end

    end
  end
end
