module DynaModel
  module Document

    MAX_ITEM_SIZE = 65_536
    # These delimiters are also reserved characters and should not be used in
    # hash or range keys
    GUID_DELIMITER_PRECEDENCE = ["_", ":", "|", ",", "!", "~", "@", "^"]

    extend ActiveSupport::Concern

    included do
      class_attribute :read_only_attributes, :base_class
      #self.read_only_attributes = []
      self.base_class = self

      AWS::Record.table_prefix = "#{Rails.application.class.parent_name.to_s.underscore.dasherize}-#{Rails.env}-"

      DynaModel::Config.included_models << self
      extend ActiveModel::Translation
      extend ActiveModel::Callbacks
      extend AWS::Record::AbstractBase
      include DynaModel::Persistence

      define_model_callbacks :create, :save, :destroy, :initialize, :update

      #before_create :set_created_at
      #before_save :set_updated_at
      #after_initialize :set_type
    end

    #include ActiveModel::AttributeMethods
    include ActiveModel::Conversion
    include ActiveModel::MassAssignmentSecurity if defined?(ActiveModel::MassAssignmentSecurity)
    include ActiveModel::Naming
    include ActiveModel::Observing if defined?(ActiveModel::Observing)
    include ActiveModel::Serializers::JSON
    include ActiveModel::Serializers::Xml
    
    include DynaModel::Attributes
    include DynaModel::Schema
    include DynaModel::Query

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
          raise "Table #{table_name} already exists"
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
