module DynaModel
  class Table

    extend AWS::DynamoDB::Types

    attr_reader :table_schema, :client, :schema_loaded_from_dynamo, :hash_key, :range_keys

    RETURNED_CONSUMED_CAPACITY = {
      none: "NONE",
      total: "TOTAL"
    }

    TYPE_INDICATOR = {
      b: "B",
      n: "N",
      s: "S",
      ss: "SS",
      ns: "NS"
    }

    RETURN_VALUES = {
      none: "NONE",
      all_old: "ALL_OLD",
      updated_old: "UPDATED_OLD",
      all_new: "ALL_NEW",
      updated_new: "UPDATED_NEW"
    }

    RETURN_VALUES_UPDATE_ONLY = [
      :updated_old,
      :all_new,
      :updated_new
    ]

    QUERY_SELECT = {
      all: "ALL_ATTRIBUTES",
      projected: "ALL_PROJECTED_ATTRIBUTES",
      count: "COUNT",
      specific: "SPECIFIC_ATTRIBUTES"
    }

    COMPARISON_OPERATOR = {
      eq: "EQ",
      le: "LE",
      lt: "LT",
      ge: "GE",
      gt: "GT",
      begins_with: "BEGINS_WITH",
      between: "BETWEEN",
      # Scan only
      ne: "NE",
      not_null: "NOT_NULL",
      null: "NULL",
      contains: "CONTAINS",
      not_contains: "NOT_CONTAINS",
      in: "IN"
    }

    CONDITIONAL_OPERATOR = {
      and: "AND",
      or: "OR"
    }

    COMPARISON_OPERATOR_SCAN_ONLY = [
      :ne, 
      :not_null,
      :null,
      :contains,
      :not_contains,
      :in
    ]

    def self.type_from_value(value)
      case
      when value.kind_of?(AWS::DynamoDB::Binary) then :b
      when value.respond_to?(:to_str) then :s
      when value.kind_of?(Numeric) then :n
      else
        raise ArgumentError, "unsupported attribute type #{value.class}"
      end
    end

    def initialize(model)
      @model = model
      @table_schema = model.table_schema
      self.load_schema
      self.validate_key_schema
    end

    def load_schema
      @schema_loaded_from_dynamo = @model.describe_table

      @schema_loaded_from_dynamo[:table][:key_schema].each do |key|
        key_attr = @table_schema[:attribute_definitions].find{|h| h[:attribute_name] == key[:attribute_name]}
        next if key_attr.nil?
        key_schema_attr = {
          attribute_name: key[:attribute_name],
          attribute_type: key_attr[:attribute_type]
        }

        if key[:key_type] == "HASH"
          @hash_key = key_schema_attr
        else
          (@range_keys ||= []) << key_schema_attr.merge(:primary_range_key => true)
          @primary_range_key = key_schema_attr.merge(:primary_range_key => true)
        end
      end

      if @schema_loaded_from_dynamo[:table][:local_secondary_indexes] || @schema_loaded_from_dynamo[:table][:global_secondary_indexes]
        ((@schema_loaded_from_dynamo[:table][:local_secondary_indexes] || []) + (@schema_loaded_from_dynamo[:table][:global_secondary_indexes] || [])).each do |key|
          si_range_key = key[:key_schema].find{|h| h[:key_type] == "RANGE" }
          next if si_range_key.nil?
          si_range_attribute = @table_schema[:attribute_definitions].find{|h| h[:attribute_name] == si_range_key[:attribute_name]}
          next if si_range_attribute.nil?
          (@range_keys ||= []) << {
            attribute_name: si_range_key[:attribute_name],
            attribute_type: si_range_attribute[:attribute_type],
            index_name: key[:index_name]
          }
        end
      end

      @schema_loaded_from_dynamo
    end

    def validate_key_schema
      if @schema_loaded_from_dynamo[:table][:key_schema].sort_by { |k| k[:key_type] }.map(&:to_h) != @table_schema[:key_schema].sort_by { |k| k[:key_type] }
        raise ArgumentError, "It appears your key schema (Hash Key/Range Key) have changed from the table definition. Rebuilding the table is necessary."
      end

      if @schema_loaded_from_dynamo[:table][:attribute_definitions].sort_by { |k| k[:attribute_name] }.map(&:to_h) != @table_schema[:attribute_definitions].sort_by { |k| k[:attribute_name] }
        raise ArgumentError, "It appears your attribute definition (types?) have changed from the table definition. Rebuilding the table is necessary."
      end

      index_keys_to_reject = [:index_status, :index_size_bytes, :item_count]

      if @schema_loaded_from_dynamo[:table][:local_secondary_indexes].blank? != @table_schema[:local_secondary_indexes].blank?
        raise ArgumentError, "It appears your local secondary indexes have changed from the table definition. Rebuilding the table is necessary."
      end
      
      if @schema_loaded_from_dynamo[:table][:local_secondary_indexes] && (@schema_loaded_from_dynamo[:table][:local_secondary_indexes].map(&:to_h).collect {|i| i.delete_if{|k, v| index_keys_to_reject.include?(k) }; i }.sort_by { |lsi| lsi[:index_name] } != @table_schema[:local_secondary_indexes].sort_by { |lsi| lsi[:index_name] })
        raise ArgumentError, "It appears your local secondary indexes have changed from the table definition. Rebuilding the table is necessary."
      end

      if @schema_loaded_from_dynamo[:table][:global_secondary_indexes].blank? != @table_schema[:global_secondary_indexes].blank?
        raise ArgumentError, "It appears your global secondary indexes have changed from the table definition. Rebuilding the table is necessary."
      end

      if @schema_loaded_from_dynamo[:table][:global_secondary_indexes] && (@schema_loaded_from_dynamo[:table][:global_secondary_indexes].map(&:to_h).collect {|i| i.delete_if{|k, v| index_keys_to_reject.include?(k) }; i }.sort_by { |gsi| gsi[:index_name] } != @table_schema[:global_secondary_indexes].sort_by { |gsi| gsi[:index_name] })
        raise ArgumentError, "It appears your global secondary indexes have changed from the table definition. Rebuilding the table is necessary."
      end

      if @schema_loaded_from_dynamo[:table][:provisioned_throughput][:read_capacity_units] != @table_schema[:provisioned_throughput][:read_capacity_units]
        DynaModel::Config.logger.error "read_capacity_units mismatch. Need to update table?"
      end

      if @schema_loaded_from_dynamo[:table][:provisioned_throughput][:write_capacity_units] != @table_schema[:provisioned_throughput][:write_capacity_units]
        DynaModel::Config.logger.error "write_capacity_units mismatch. Need to update table?"
      end
    end

    def hash_key_item_param(value)
      { @table_schema[:key_schema].find{|h| h[:key_type] == "HASH"}[:attribute_name] => value }
    end

    def hash_key_condition_param(hash_key, value)
      {
        hash_key => {
          attribute_value_list: [value],
          comparison_operator: COMPARISON_OPERATOR[:eq]
        }
      }
    end

    def get_item(hash_key, options={})
      options[:consistent_read] = false unless options[:consistent_read]
      options[:return_consumed_capacity] ||= :none # "NONE" # || "TOTAL"
      options[:select] ||= [] # no :projected option, always an array or :all
      raise ArgumentError, "Invalid :select. GetItem :select must be an Array (blank for :all)" unless options[:select].is_a?(Array)

      get_item_request = {
        table_name: @model.dynamo_db_table_name(options[:shard_name]),
        key: hash_key_item_param(hash_key),
        consistent_read: options[:consistent_read],
        return_consumed_capacity: RETURNED_CONSUMED_CAPACITY[options[:return_consumed_capacity]]
      }
      if options[:select].blank?
        options[:select] = :all # for obj_from_attrs
      else
        get_item_request.merge!( attributes_to_get: [options[:select]].flatten )
      end

      @model.dynamo_db_client.get_item(get_item_request)
    end

    # == options
    #    * consistent_read
    #    * return_consumed_capacity
    #    * order
    #    * select
    #    * range
    def query(hash_value, options={})
      options[:consistent_read] = false unless options[:consistent_read]
      options[:return_consumed_capacity] ||= :none # "NONE" # || "TOTAL"
      options[:order] ||= :desc
      #options[:index_name] ||= :none
      #AWS::DynamoDB::Errors::ValidationException: ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName
      #options[:limit] ||= 10
      #options[:exclusive_start_key]
      #options[:query_filter]

      key_conditions = {}
      gsi = nil
      if options[:global_secondary_index]
        gsi = @table_schema[:global_secondary_indexes].select{ |gsi| gsi[:index_name].to_s == options[:global_secondary_index].to_s}.first
        raise ArgumentError, "Could not find Global Secondary Index '#{options[:global_secondary_index]}'" unless gsi
        gsi_hash_key = gsi[:key_schema].find{|h| h[:key_type] == "HASH"}[:attribute_name]
        key_conditions.merge!(hash_key_condition_param(gsi_hash_key, hash_value))
      else
        hash_key = @table_schema[:key_schema].find{|h| h[:key_type] == "HASH"}[:attribute_name]
        key_conditions.merge!(hash_key_condition_param(hash_key, hash_value))
      end

      query_request = {
        table_name: @model.dynamo_db_table_name(options[:shard_name]),
        key_conditions: key_conditions,
        consistent_read: options[:consistent_read],
        return_consumed_capacity: RETURNED_CONSUMED_CAPACITY[options[:return_consumed_capacity]],
        scan_index_forward: (options[:order] == :asc)
      }

      if options[:range] 
        raise ArgumentError, "Table does not use a range key in its schema!" if @range_keys.blank?
        attr_with_condition_hash = self.attr_with_condition(options[:range])
        range_key = @range_keys.find{|k| k[:attribute_name] == attr_with_condition_hash.keys.first}
        raise ArgumentError, ":range key must be a valid Range attribute" unless range_key

        if range_key.has_key?(:index_name) # Local/Global Secondary Index
          options[:index_name] = range_key[:index_name]
          query_request[:index_name] = range_key[:index_name]
        end

        key_conditions.merge!(attr_with_condition_hash)
      end

      query_filter = {}
      conditional_operator = nil
      if options[:query_filter]
        raise ArgumentError, ":query_filter must be a hash" unless options[:query_filter].is_a?(Hash)
        options[:query_filter].each_pair do |k,v| 
          query_filter.merge!(self.attr_with_condition({ k => v}))
        end
        if options[:conditional_operator]
          raise ArgumentError, ":condition_operator invalid! Must be one of (#{CONDITIONAL_OPERATOR.keys.join(", ")})" unless CONDITIONAL_OPERATOR[options[:conditional_operator]]
          conditional_operator = CONDITIONAL_OPERATOR[options[:conditional_operator]]
        end
      end
      query_request.merge!(query_filter: query_filter) unless query_filter.blank?
      query_request.merge!(conditional_operator: conditional_operator) unless conditional_operator.blank? || query_filter.blank?

      if options[:global_secondary_index] # Override index_name if using GSI
        # You can only select projected attributes from a GSI
        options[:select] = :projected #if options[:select].blank?
        options[:index_name] = gsi[:index_name]
        query_request.merge!(index_name: gsi[:index_name])
      end
      options[:select] ||= :all # :all, :projected, :count, []
      if options[:select].is_a?(Array)
        attrs_to_select = [options[:select].map(&:to_s)].flatten
        attrs_to_select << @hash_key[:attribute_name]
        attrs_to_select << @primary_range_key[:attribute_name] if @primary_range_key
        query_request.merge!({
          select: QUERY_SELECT[:specific],
          attributes_to_get: attrs_to_select.uniq
        })
      else
        query_request.merge!({ select: QUERY_SELECT[options[:select]] })
      end
      
      query_request.merge!({ limit: options[:limit].to_i }) if options.has_key?(:limit)
      query_request.merge!({ exclusive_start_key: options[:exclusive_start_key] }) if options[:exclusive_start_key]

      @model.dynamo_db_client.query(query_request)
    end

    def batch_get_item(keys, options={})
      options[:return_consumed_capacity] ||= :none
      options[:select] ||= [] # no :projected option, always an array or :all
      options[:consistent_read] = false unless options[:consistent_read]

      raise ArgumentError, "must include between 1 - 100 keys" if keys.size == 0 || keys.size > 100
      keys_request = []
      keys.each do |k|
        key_request = {}
        if @primary_range_key
          hash_value, range_value = k.split(@model.guid_delimiter)
        else
          hash_value = k
        end
        key_request[@hash_key[:attribute_name]] = hash_value
        if @primary_range_key
          raise ArgumentError, "every key must include a range_value" if range_value.blank?
          key_request[@primary_range_key[:attribute_name]] = range_value
        end
        keys_request << key_request
      end

      request_items_request = {}
      request_items_request.merge!( keys: keys_request )
      if options[:select].blank?
        options[:select] = :all # for obj_from_attrs
      else
        request_items_request.merge!( attributes_to_get: [options[:select]].flatten )
      end
      request_items_request.merge!( consistent_read: options[:consistent_read] ) if options[:consistent_read]
      batch_get_item_request = {
        request_items: { @model.dynamo_db_table_name(options[:shard_name]) => request_items_request },
        return_consumed_capacity: RETURNED_CONSUMED_CAPACITY[options[:return_consumed_capacity]]
      }
      @model.dynamo_db_client.batch_get_item(batch_get_item_request)
    end

    def write(attributes, options={})
      options[:return_consumed_capacity] ||= :none
      options[:return_values] ||= :none
      options[:update_item] = false unless options[:update_item]

      expected = {}
      conditional_operator = nil
      if options[:expected]
        raise ArgumentError, ":expected must be a hash" unless options[:expected].is_a?(Hash)
        options[:expected].each_pair do |k,v| 
          expected.merge!(self.attr_with_condition({ k => v }))
        end
        if options[:conditional_operator]
          raise ArgumentError, ":condition_operator invalid! Must be one of (#{CONDITIONAL_OPERATOR.keys.join(", ")})" unless CONDITIONAL_OPERATOR[options[:conditional_operator]]
          conditional_operator = CONDITIONAL_OPERATOR[options[:conditional_operator]]
        end
      end

      if options[:update_item]
        # UpdateItem
        key_request = { @hash_key[:attribute_name] => options[:update_item][:hash_value] }
        if @primary_range_key
          raise ArgumentError, "range_key was not provided to the write command" if options[:update_item][:range_value].blank?
          key_request.merge!({ @primary_range_key[:attribute_name] => options[:update_item][:range_value] })
        end
        attrs_to_update = {}
        attributes.each_pair do |k,v|
          next if k == @hash_key[:attribute_name] || (@primary_range_key && k == @primary_range_key[:attribute_name])
          if v.nil?
            attrs_to_update.merge!({ k => { :action => "DELETE" } })
          else
            attrs_to_update.merge!({
              k => {
                value: v,
                action: "PUT"
              }
            })
          end
        end
        raise ArgumentError, ":return_values must be one of (#{RETURN_VALUES.keys.join(", ")})" unless RETURN_VALUES[options[:return_values]]
        update_item_request = {
          table_name: @model.dynamo_db_table_name(options[:shard_name]),
          key: key_request,
          attribute_updates: attrs_to_update,
          return_consumed_capacity: RETURNED_CONSUMED_CAPACITY[options[:return_consumed_capacity]],
          return_values: RETURN_VALUES[options[:return_values]]
        }
        update_item_request.merge!(expected: expected) unless expected.blank?
        update_item_request.merge!(conditional_operator: conditional_operator) unless conditional_operator.blank? || expected.blank?
        @model.dynamo_db_client.update_item(update_item_request)
      else
        # PutItem
        items = {}
        attributes.each_pair do |k,v|
          next if v.blank? # If empty string or nil, skip...
          items.merge!({ k => v })
        end
        raise ArgumentError, ":return_values must be one of (#{(RETURN_VALUES.keys - RETURN_VALUES_UPDATE_ONLY).join(", ")})" unless RETURN_VALUES[options[:return_values]] && !RETURN_VALUES_UPDATE_ONLY.include?(options[:return_values])
        put_item_request = {
          table_name: @model.dynamo_db_table_name(options[:shard_name]),
          item: items,
          return_consumed_capacity: RETURNED_CONSUMED_CAPACITY[options[:return_consumed_capacity]],
          return_values: RETURN_VALUES[options[:return_values]]
        }
        put_item_request.merge!(expected: expected) unless expected.blank?
        put_item_request.merge!(conditional_operator: conditional_operator) unless conditional_operator.blank? || expected.blank?
        @model.dynamo_db_client.put_item(put_item_request)
      end
    end

    def delete_item(options={})
      raise ":delete_item => {...key_values...} required" unless options[:delete_item].present?
      options[:return_consumed_capacity] ||= :none
      options[:return_values] ||= :none
      raise ArgumentError, ":return_values must be one of (#{(RETURN_VALUES.keys - RETURN_VALUES_UPDATE_ONLY).join(", ")})" unless RETURN_VALUES[options[:return_values]] && !RETURN_VALUES_UPDATE_ONLY.include?(options[:return_values])
      key_request = { @hash_key[:attribute_name] => options[:delete_item][:hash_value] }

      if @primary_range_key
        raise ArgumentError, "range_key was not provided to the delete_item command" if options[:delete_item][:range_value].blank?
        key_request.merge!({ @primary_range_key[:attribute_name] => options[:delete_item][:range_value] })
      end

      expected = {}
      conditional_operator = nil
      if options[:expected]
        raise ArgumentError, ":expected must be a hash" unless options[:expected].is_a?(Hash)
        options[:expected].each_pair do |k,v| 
          expected.merge!(self.attr_with_condition({ k => v }))
        end
        if options[:conditional_operator]
          raise ArgumentError, ":condition_operator invalid! Must be one of (#{CONDITIONAL_OPERATOR.keys.join(", ")})" unless CONDITIONAL_OPERATOR[options[:conditional_operator]]
          conditional_operator = CONDITIONAL_OPERATOR[options[:conditional_operator]]
        end
      end

      delete_item_request = {
        table_name: @model.dynamo_db_table_name(options[:shard_name]),
        key: key_request,
        return_consumed_capacity: RETURNED_CONSUMED_CAPACITY[options[:return_consumed_capacity]],
        return_values: RETURN_VALUES[options[:return_values]]
      }
      delete_item_request.merge!(expected: expected) unless expected.blank?
      delete_item_request.merge!(conditional_operator: conditional_operator) unless conditional_operator.blank? || expected.blank?
      @model.dynamo_db_client.delete_item(delete_item_request)
    end

    # Perform a table scan
    # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
    def scan(options={})
      options[:return_consumed_capacity] ||= :none # "NONE" # || "TOTAL"
      # Default if not already set
      options[:select] ||= :all # :all, :projected, :count, []

      scan_request = {
        table_name: @model.dynamo_db_table_name(options[:shard_name]),
        return_consumed_capacity: RETURNED_CONSUMED_CAPACITY[options[:return_consumed_capacity]]
      }

      scan_request.merge!({ limit: options[:limit].to_i }) if options.has_key?(:limit)
      scan_request.merge!({ exclusive_start_key: options[:exclusive_start_key] }) if options[:exclusive_start_key]

      if options[:select].is_a?(Array)
        attrs_to_select = [options[:select].map(&:to_s)].flatten
        attrs_to_select << @hash_key[:attribute_name]
        attrs_to_select << @primary_range_key[:attribute_name] if @primary_range_key
        scan_request.merge!({
          select: QUERY_SELECT[:specific],
          attributes_to_get: attrs_to_select.uniq
        })
      else
        scan_request.merge!({ select: QUERY_SELECT[options[:select]] })
      end

      # :scan_filter => { :name.begins_with => "a" }
      scan_filter = {}
      conditional_operator = nil
      if options[:scan_filter].present?
        options[:scan_filter].each_pair.each do |k,v|
          scan_filter.merge!(self.attr_with_condition({ k => v}))
        end
      end
      if options[:conditional_operator]
        raise ArgumentError, ":condition_operator invalid! Must be one of (#{CONDITIONAL_OPERATOR.keys.join(", ")})" unless CONDITIONAL_OPERATOR[options[:conditional_operator]]
        conditional_operator = CONDITIONAL_OPERATOR[options[:conditional_operator]]
      end
      scan_request.merge!(scan_filter: scan_filter) unless scan_filter.blank?
      scan_request.merge!(conditional_operator: conditional_operator) unless conditional_operator.blank? || scan_filter.blank?
      scan_request.merge!(segment: options[:segment].to_i) if options[:segment].present?
      scan_request.merge!(total_segments: options[:total_segments].to_i) if options[:total_segments].present?

      @model.dynamo_db_client.scan(scan_request)
    end

    protected
    def cast_hash_value
      hash_key_type = @table_schema[:attribute_definitions].find{|h| h[:attribute_name] == hash_key}[:attribute_type]
    end

    def cast_range_value
      hash_key_type = @table_schema[:attribute_definitions].find{|h| h[:attribute_name] == hash_key}[:attribute_type]
    end

    # {:name.eq => "cary"}
    #
    # return:
    #   {
    #     "name" => {
    #       attribute_value_list: [
    #         "S" => "cary"
    #       ],
    #       comparison_operator: "EQ"
    #     }
    #   }
    def attr_with_condition(attr_conditional)
      raise ArgumentError, "Expected a 2 element Hash for each :query_filter (ex {:age.gt => 13})" unless attr_conditional.is_a?(Hash) && attr_conditional.keys.size == 1 && attr_conditional.keys.first.is_a?(String)
      attr_name, comparison_operator = attr_conditional.keys.first.split(".")
      raise ArgumentError, "Comparison operator must be one of (#{(COMPARISON_OPERATOR.keys - COMPARISON_OPERATOR_SCAN_ONLY).join(", ")})" unless COMPARISON_OPERATOR.keys.include?(comparison_operator.to_sym)
      attr_key = @model.attributes[attr_name]
      attr_class = attr_key.class
      raise ArgumentError, "#{attr_name} not a valid attribute" unless attr_key
      attr_type = @model.attribute_type_indicator(attr_key)
      raise ArgumentError, "#{attr_name} key must be a Range if using the operator BETWEEN" if comparison_operator == "between" && !attr_conditional.values.first.is_a?(Range)
      raise ArgumentError, ":query_filter value must be an Array if using the operator IN" if comparison_operator == "in" && !attr_conditional.values.first.is_a?(Array)

      attr_value = attr_conditional.values.first

      attribute_value_list = []
      if comparison_operator == "in"
        attr_value.each do |in_v|
          attribute_value_list << casted_attr_value(attr_class, in_v)
        end
      elsif comparison_operator == "between"
        attribute_value_list << attr_value.min
        attribute_value_list << attr_value.max
      else
        attribute_value_list = [casted_attr_value(attr_class, attr_value)]
      end

      attribute_comparison_hash = {
        comparison_operator: COMPARISON_OPERATOR[comparison_operator.to_sym]
      }
      attribute_comparison_hash.merge!(attribute_value_list: attribute_value_list) unless %w(null not_null).include?(comparison_operator)

      { attr_name => attribute_comparison_hash }
    end

    def casted_attr_value(attr_class, val)
      casted = attr_class.type_cast(val)
      return nil if casted.nil?
      attr_class.serialize(casted)
    end

  end
end
