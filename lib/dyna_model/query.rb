module DynaModel
  module Query
    extend ActiveSupport::Concern

    # Failsafe
    QUERY_TIMEOUT = 30 # seconds
    DEFAULT_BATCH_SIZE = 100

    module ClassMethods

      def read_guid(guid, options={})
        return nil if guid.blank?
        if self.range_key
          hash_value, range_value = guid.split(self.guid_delimiter)
          self.read(hash_value, range_value, options)
        else
          self.read(guid, options)
        end
      end

      def read(hash_value, range_value_or_options=nil, options=nil)
        if self.range_key.nil?
          item_attrs = self.dynamo_db_table.get_item(hash_value, range_value_or_options || {})[:item]
          return nil if item_attrs.nil?
          self.obj_from_attrs(item_attrs, (range_value_or_options || {}))
        else
          raise ArgumentError, "This table requires a range_key_value" if range_value_or_options.nil?
          self.read_range(hash_value, (options || {}).merge(range: { self.range_key[:attribute_name].to_sym.eq => range_value_or_options})).first
        end
      end

      def read_multiple(keys, options={})
        options[:format] = (options[:format] && options[:format] == :array) ? :array : :hash
        results_map = {}
        results_arr = []
        if keys.present?
          response = self.dynamo_db_table.batch_get_item(keys, options)
          response.responses[self.dynamo_db_table_name(options[:shard_name])].each do |result|
            obj = self.obj_from_attrs(result, options)
            if options[:format] == :array
              results_arr << obj
            else
              if self.dynamo_db_table.range_keys.present? && primary_range_key = self.dynamo_db_table.range_keys.find{|rk| rk[:primary_range_key] }
                (results_map[obj.send(:[], self.dynamo_db_table.hash_key[:attribute_name])] ||= {})[obj.send(:[], primary_range_key[:attribute_name])] = obj
              else
                results_map[obj.send(:[], self.dynamo_db_table.hash_key[:attribute_name])] = obj
              end
            end
          end
        end
        options[:format] == :array ? results_arr : results_map
      end

      # Read results up to the limit
      #   read_range("1", :range => { :varname.gte => "2"}, :limit => 10)
      # Loop results in given batch size until limit is hit or no more results
      #   read_range("1", :range => { :varname.eq => "2"}, :batch => 10, :limit => 1000)
      def read_range(hash_value, options={})
        raise ArgumentError, "no range_key specified for this table" if self.dynamo_db_table.range_keys.blank? && self.global_secondary_indexes.blank?
        aggregated_results = []

        # Useful if doing pagination where you would need the last key evaluated
        return_last_evaluated_key = options.delete(:return_last_evaluated_key)
        batch_size = options.delete(:batch) || DEFAULT_BATCH_SIZE
        max_results_limit = options[:limit]
        if options[:limit] && options[:limit] > batch_size
          options.merge!(limit: batch_size)
        end

        response = self.dynamo_db_table.query(hash_value, options)
        response.items.each do |result|
          aggregated_results << self.obj_from_attrs(result, options)
        end

        if response.last_evaluated_key
          results_returned = response.count
          batch_iteration = 0
          Timeout::timeout(QUERY_TIMEOUT) do
            while response.last_evaluated_key
              if max_results_limit && (delta_results_limit = (max_results_limit-results_returned)) < batch_size
                break if delta_results_limit == 0
                options.merge!(limit: delta_results_limit)
              else
                options.merge!(limit: batch_size)
              end

              response = self.dynamo_db_table.query(hash_value, options.merge(exclusive_start_key: response.last_evaluated_key))
              response.items.each do |result|
                aggregated_results << self.obj_from_attrs(result, options)
              end
              results_returned += response.count
              batch_iteration += 1
            end
          end
        end

        if return_last_evaluated_key
          {
            last_evaluated_key: response.last_evaluated_key,
            members: aggregated_results
          }
        else
          aggregated_results
        end
      end

      def count_range(hash_value, options={})
        raise ArgumentError, "no range_key specified for this table" if self.dynamo_db_table.range_keys.blank?
        response = self.dynamo_db_table.query(hash_value, options.merge(select: :count))
        response.count
      end

      def read_first(hash_value, options={})
        options[:limit] = 1
        self.read_range(hash_value, options).first
      end

      #:count=>10, :scanned_count=>10, :last_evaluated_key=>{"guid"=>{:s=>"11f82550-5c5d-11e3-9b55-d311a43114ca"}}}
      # :manual_batching => true|false
      #   return results with last_evaluated_key instead of automatically looping through (useful to throttle or )
      def scan(options={})
        aggregated_results = []

        batch_size = options.delete(:batch) || DEFAULT_BATCH_SIZE
        max_results_limit = options[:limit]
        options[:limit] = batch_size

        response = self.dynamo_db_table.scan(options)
        response.items.each do |result|
          aggregated_results << self.obj_from_attrs(result, options)
        end

        if response.last_evaluated_key && !options[:manual_batching]
          results_returned = response.count
          batch_iteration = 0
          Timeout::timeout(QUERY_TIMEOUT) do
            while response.last_evaluated_key
              if max_results_limit && (delta_results_limit = (max_results_limit-results_returned)) < batch_size
                break if delta_results_limit == 0
                options.merge!(limit: delta_results_limit)
              else
                options.merge!(limit: batch_size)
              end

              response = dynamo_table.scan(options.merge(exclusive_start_key: response.last_evaluated_key))
              response.items.each do |result|
                aggregated_results << self.obj_from_attrs(result, options)
              end
              results_returned += response.count
              batch_iteration += 1
            end
          end
        end

        if options[:manual_batching]
          response_hash = {
            results: aggregated_results,
            last_evaluated_key: response.last_evaluated_key
          }
          response_hash.merge!(consumed_capacity: response.consumed_capacity) if response.consumed_capacity
          response_hash
        else
          aggregated_results
        end
      end # scan

      protected
      def obj_from_attrs(attrs, options={})
        obj = self.new(shard: self.shard_name(options[:shard_name]))
        obj.send(:hydrate, nil, attrs)
        if options[:select]
          obj.instance_variable_set("@_select", options[:select])
          if options[:select] != :all
            #:all, :projected, :count, :specific
            selected_attrs = []
            # Primary hash/range key are always returned...
            self.table_schema[:key_schema].each do |k|
              selected_attrs << k[:attribute_name]
            end
            if options[:select] == :projected
              index = ((self.table_schema[:global_secondary_indexes] || []) + (self.table_schema[:local_secondary_indexes] || [])).find { |i| i[:index_name] == options[:index_name].to_s }
              raise "Index '#{options[:index_name]}' not found in table schema" unless index
              index[:key_schema].each do |k|
                selected_attrs << k[:attribute_name].to_s
              end
              if index[:projection] && index[:projection][:non_key_attributes]
                index[:projection][:non_key_attributes].each do |a|
                  selected_attrs << a.to_s
                end
              end
            elsif options[:select].is_a?(Array)
              obj.instance_variable_set("@_select", :specific)
              selected_attrs += options[:select].map(&:to_s)
            end
            selected_attrs.uniq!
            obj.instance_variable_set("@_selected_attributes", selected_attrs.compact)
          end
        end
        obj
      end

    end # ClassMethods

  end
end
