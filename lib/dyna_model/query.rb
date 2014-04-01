module DynaModel
  module Querying
    extend ActiveSupport::Concern

    # Failsafe
    QUERY_TIMEOUT = 30 # seconds
    DEFAULT_BATCH_SIZE = 100

    module ClassMethods

      def read(hash_value, range_value_or_options=nil, options=nil)
        
      end

      # Read results up to the limit
      #   read_range("1", :range => { :varname.gte => "2"}, :limit => 10)
      # Loop results in given batch size until limit is hit or no more results
      #   read_range("1", :range => { :varname.eq => "2"}, :batch => 10, :limit => 1000)
      def read_range(hash_value, options={})
        raise ArgumentError, "no range_key specified for this table" if dynamo_table.range_keys.blank? && global_secondary_indexes.blank?
        aggregated_results = []

        # Useful if doing pagination where you would need the last key evaluated
        return_last_evaluated_key = options.delete(:return_last_evaluated_key)
        batch_size = options.delete(:batch) || DEFAULT_BATCH_SIZE
        max_results_limit = options[:limit]
        if options[:limit] && options[:limit] > batch_size
          options.merge!(:limit => batch_size)
        end

        results = dynamo_table.query(hash_value, options)
        response = Response.new(results)

        results[:member].each do |result|
          attrs = Response.strip_attr_types(result)
          aggregated_results << load(attrs[dynamo_table.hash_key[:attribute_name]], attrs)
        end

        if response.more_results?
          results_returned = response.count
          batch_iteration = 0
          while response.more_results? && batch_iteration < MAX_BATCH_ITERATIONS
            if max_results_limit && (delta_results_limit = (max_results_limit-results_returned)) < batch_size
              break if delta_results_limit == 0
              options.merge!(:limit => delta_results_limit)
            else
              options.merge!(:limit => batch_size)
            end

            results = dynamo_table.query(hash_value, options.merge(:exclusive_start_key => response.last_evaluated_key))
            response = Response.new(results)
            results[:member].each do |result|
              attrs = Response.strip_attr_types(result)
              aggregated_results << load(attrs[dynamo_table.hash_key[:attribute_name]], attrs)
            end
            results_returned += response.count
            batch_iteration += 1
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
        raise ArgumentError, "no range_key specified for this table" if dynamo_table.range_keys.blank?
        results = dynamo_table.query(hash_value, options.merge(:select => :count))
        Response.new(results).count
      end

      def read_multiple(keys, options=nil)
        results_map = {}
        results = adapter.batch_read(keys, options)
        results[:responses][dynamo_table.table_schema[:table_name]].each do |result|
          attrs = Response.strip_attr_types(result)
          if dynamo_table.range_keys.present? && primary_range_key = dynamo_table.range_keys.find{|rk| rk[:primary_range_key] }
            (results_map[attrs[dynamo_table.hash_key[:attribute_name]]] ||= {})[attrs[primary_range_key[:attribute_name]]] = load(attrs[dynamo_table.hash_key[:attribute_name]], attrs)
          else
            results_map[attrs[dynamo_table.hash_key[:attribute_name]]] = load(attrs[dynamo_table.hash_key[:attribute_name]], attrs)
          end
        end
        results_map
      end

      #:count=>10, :scanned_count=>10, :last_evaluated_key=>{"guid"=>{:s=>"11f82550-5c5d-11e3-9b55-d311a43114ca"}}}
      # :manual_batching => true|false
      #   return results with last_evaluated_key instead of automatically looping through (useful to throttle or )
      def scan(options={})
        aggregated_results = []

        batch_size = options.delete(:batch) || DEFAULT_BATCH_SIZE
        max_results_limit = options[:limit]
        options[:limit] = batch_size

        results = dynamo_table.scan(options)
        response = Response.new(results)

        results[:member].each do |result|
          attrs = Response.strip_attr_types(result)
          aggregated_results << load(attrs[dynamo_table.hash_key[:attribute_name]], attrs)
        end

        if response.more_results? && !options[:manual_batching]
          results_returned = response.count
          batch_iteration = 0
          Timeout::timeout(QUERY_TIMEOUT) do
            while response.more_results?
              if max_results_limit && (delta_results_limit = (max_results_limit-results_returned)) < batch_size
                break if delta_results_limit == 0
                options.merge!(limit: delta_results_limit)
              else
                options.merge!(limit: batch_size)
              end

              results = dynamo_table.scan(options.merge(exclusive_start_key: response.last_evaluated_key))
              response = Response.new(results)
              results[:member].each do |result|
                attrs = Response.strip_attr_types(result)
                aggregated_results << load(attrs[dynamo_table.hash_key[:attribute_name]], attrs)
              end
              results_returned += response.count
              batch_iteration += 1
            end
          end
        end

        if options[:manual_batching]
          response_hash = {
            results: aggregated_results,
            last_evaluated_key: results[:last_evaluated_key]
          }
          response_hash.merge!(consumed_capacity: results[:consumed_capacity]) if results[:consumed_capacity]
          response_hash
        else
          aggregated_results
        end
      end # scan

      def read_first(hash_value, options={})
        self.read_range(hash_value, options).first
      end

    end # ClassMethods

  end
end
