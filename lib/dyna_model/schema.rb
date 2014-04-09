module DynaModel
  module Schema
    extend ActiveSupport::Concern

    module ClassMethods

      KEY_TYPE = {
        hash: "HASH",
        range: "RANGE"
      }

      PROJECTION_TYPE = {
        keys_only: "KEYS_ONLY",
        all: "ALL",
        include: "INCLUDE"
      }

      ATTR_TYPES = {
        AWS::Record::Attributes::StringAttr => "S",
        AWS::Record::Attributes::IntegerAttr => "N",
        AWS::Record::Attributes::FloatAttr => "N",
        AWS::Record::Attributes::BooleanAttr => "S",
        AWS::Record::Attributes::DateTimeAttr => "S",
        AWS::Record::Attributes::DateAttr => "S",
        AWS::Record::Attributes::SerializedAttr => "B"
      }

      def table_schema
        schema = {
          table_name: dynamo_db_table_name,
          provisioned_throughput: {
            read_capacity_units: read_provision,
            write_capacity_units: write_provision
          },
          key_schema: key_schema,
          attribute_definitions: attribute_definitions
        }
        schema[:local_secondary_indexes] = local_secondary_indexes unless local_secondary_indexes.blank?
        schema[:global_secondary_indexes] = global_secondary_indexes unless global_secondary_indexes.blank?
        schema
      end

      def guid_delimiter(val=nil)
        if val
          raise(ArgumentError, "Invalid guid_delimiter") if val.blank?
          @guid_delimiter = val.to_s
        else
          @guid_delimiter || DynaModel::Config.default_guid_delimiter
        end
      end

      def read_provision(val=nil)
        if val
          raise(ArgumentError, "Invalid read provision") unless val.to_i >= 1
          @dynamo_read_provision = val.to_i
        else
          @dynamo_read_provision || DynaModel::Config.read_provision
        end
      end

      def write_provision(val=nil)
        if val
          raise(ArgumentError, "Invalid write provision") unless val.to_i >= 1
          @dynamo_write_provision = val.to_i
        else
          @dynamo_write_provision || DynaModel::Config.write_provision
        end
      end

      def table_prefix(val=nil)
        if val
          @dynamo_table_prefix = val
        else
          @dynamo_table_prefix || AWS::Record.table_prefix || "#{DynaModel::Config.namespace}#{Rails.application.class.parent_name.to_s.underscore.dasherize}-#{Rails.env}-"
        end
      end

      def hash_key(hash_key_key=nil)
        if hash_key_key
          hash_key_attribute = self.attributes[hash_key_key.to_s]
          raise(ArgumentError, "Could not find attribute definition for hash_key #{hash_key_key}") unless hash_key_attribute
          raise(ArgumentError, "Invalid attribute type for hash_key") unless [AWS::Record::Attributes::StringAttr, AWS::Record::Attributes::IntegerAttr, AWS::Record::Attributes::FloatAttr].include?(hash_key_attribute.class)

          validates_presence_of hash_key_attribute.name.to_sym

          @dynamo_hash_key = {
            attribute_name: hash_key_attribute.name,
            key_type: KEY_TYPE[:hash]
          }
        else
          @dynamo_hash_key
        end
      end

      def range_key(range_key_key=nil)
        if range_key_key
          range_key_attribute = self.attributes[range_key_key.to_s]
          raise(ArgumentError, "Could not find attribute definition for range_key #{range_key_key}") unless range_key_attribute
          raise(ArgumentError, "Invalid attribute type for range_key") unless [AWS::Record::Attributes::StringAttr, AWS::Record::Attributes::IntegerAttr, AWS::Record::Attributes::FloatAttr].include?(range_key_attribute.class)

          validates_presence_of range_key_attribute.name.to_sym

          @dynamo_range_key = {
            attribute_name: range_key_attribute.name,
            key_type: KEY_TYPE[:range]
          }
        else
          @dynamo_range_key
        end
      end

      # TODO - need to add projections?
      def attribute_definitions
        # Keys for hash/range/secondary
        # S | N | B

        keys = []
        keys << hash_key[:attribute_name]
        keys << range_key[:attribute_name] if range_key
        local_secondary_indexes.each do |lsi|
          keys << lsi[:key_schema].select{|h| h[:key_type] == "RANGE"}.first[:attribute_name]
        end

        global_secondary_indexes.each do |lsi|
          lsi[:key_schema].each do |a|
            keys << a[:attribute_name]
          end
        end

        definitions = keys.uniq.collect do |k|
          attr = self.attributes[k.to_s]
          {
            attribute_name: attr.name,
            attribute_type: attribute_type_indicator(attr)
          }
        end
      end

      def attribute_type_indicator(attr)
        if attr_type = ATTR_TYPES[attr.class]
          attr_type
        else
          raise "unsupported attribute type #{attr.class}"
        end
      end

      def key_schema
        raise(ArgumentError, 'hash_key was not set for this table') if @dynamo_hash_key.blank?
        schema = [hash_key]
        schema << range_key if range_key 
        schema
      end

      def global_secondary_indexes
        @global_secondary_indexes ||= []
      end

      # { hash_key: :hash_key_here, range_key: :optional_range_key_here }
      # :name
      # :projection
      # :read_provision
      # :write_provision
      def global_secondary_index(index_name, options={})
        options[:projection] ||= :keys_only
        global_secondary_index_hash = {
          projection: {},
          provisioned_throughput: {
            read_capacity_units: options[:read_provision] || read_provision,
            write_capacity_units: options[:write_provision] || write_provision
          }
        }
        if options[:projection].is_a?(Array) && options[:projection].size > 0
          options[:projection].each do |non_key_attr|
            attr = self.attributes[non_key_attr.to_s]
            raise(ArgumentError, "Could not find attribute definition for projection on #{non_key_attr}") unless attr
            (global_secondary_index_hash[:projection][:non_key_attributes] ||= []) << attr.name
          end
          global_secondary_index_hash[:projection][:projection_type] = PROJECTION_TYPE[:include]
        else
          raise(ArgumentError, 'projection must be :all, :keys_only, Array (or attrs)') unless options[:projection] == :keys_only || options[:projection] == :all
          global_secondary_index_hash[:projection][:projection_type] = PROJECTION_TYPE[options[:projection]]
        end

        if !options.has_key?(:hash_key) || self.attributes[options[:hash_key].to_s].blank?
          raise(ArgumentError, "Could not find attribute definition for global secondary index on hash_key specified")
        end
        hash_key_attr = self.attributes[options[:hash_key].to_s]

        if options.has_key?(:range_key) && self.attributes[options[:range_key].to_s].blank?
          raise(ArgumentError, "Could not find attribute definition for global secondary index on range_key specified")
        end
        range_key_attr = nil
        range_key_attr = self.attributes[options[:range_key].to_s] if options.has_key?(:range_key)

        ## Force naming of index_name for lookup later
        #global_secondary_index_hash[:index_name] = (index_name.to_s || "#{hash_key_attr.name}#{"_#{range_key_attr.name}" if range_key_attr}_gsi_index".camelcase)
        global_secondary_index_hash[:index_name] = index_name.to_s

        global_secondary_index_hash[:key_schema] = [
          {
            attribute_name: hash_key_attr.name,
            key_type: KEY_TYPE[:hash]
          }
        ]
        global_secondary_index_hash[:key_schema] << {
          attribute_name: range_key_attr.name,
          key_type: KEY_TYPE[:range]
        } if range_key_attr

        return false if (@global_secondary_indexes ||= []).select {|i| i[:index_name] == global_secondary_index_hash[:index_name] }.present? # Do not add if we already have a range key set for this attr
        (@global_secondary_indexes ||= []) << global_secondary_index_hash
      end

      def local_secondary_indexes
        @local_secondary_indexes ||= []
      end

      def local_secondary_index(range_key_attr, options={})
        options[:projection] ||= :keys_only
        local_secondary_index_hash = {
          projection: {}
        }
        if options[:projection].is_a?(Array) && options[:projection].size > 0
          options[:projection].each do |non_key_attr|
            attr = self.attributes[non_key_attr.to_s]
            raise(ArgumentError, "Could not find attribute definition for projection on #{non_key_attr}") unless attr
            (local_secondary_index_hash[:projection][:non_key_attributes] ||= []) << attr.name
          end
          local_secondary_index_hash[:projection][:projection_type] = PROJECTION_TYPE[:include]
        else
          raise(ArgumentError, 'projection must be :all, :keys_only, Array (or attrs)') unless options[:projection] == :keys_only || options[:projection] == :all
          local_secondary_index_hash[:projection][:projection_type] = PROJECTION_TYPE[options[:projection]]
        end

        range_attr = self.attributes[range_key_attr.to_s]
        raise(ArgumentError, "Could not find attribute definition for local secondary index on #{range_key_attr}") unless range_attr
        local_secondary_index_hash[:index_name] = (options[:name] || options[:index_name] || "#{range_attr.name}_index".camelcase)

        hash_key_attr = self.attributes[hash_key[:attribute_name].to_s]
        raise(ArgumentError, "Could not find attribute definition for hash_key") unless hash_key_attr

        local_secondary_index_hash[:key_schema] = [
          {
            attribute_name: hash_key_attr.name,
            key_type: KEY_TYPE[:hash]
          },
          {
            attribute_name: range_attr.name,
            key_type: KEY_TYPE[:range]
          }
        ]
        return false if (@local_secondary_indexes ||= []).select {|i| i[:index_name] == local_secondary_index_hash[:index_name] }.present? # Do not add if we already have a range key set for this attr
        (@local_secondary_indexes ||= []) << local_secondary_index_hash
      end

    end # ClassMethods

  end
end
