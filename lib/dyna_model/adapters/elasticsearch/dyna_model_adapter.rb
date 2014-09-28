module Elasticsearch
  module Model
    module Adapter
      module DynaModelAdapter

        Adapter.register self, lambda { |klass| !!defined?(::DynaModel::Document) && klass.ancestors.include?(::DynaModel::Document) }

        module Records

          def records
            records_arr = klass.read_multiple(ids, format: :array)
            records_arr.sort_by { |e| response.response['hits']['hits'].index { |hit| hit['_id'].to_s == e.id.to_s } }
          end

          # Intercept call to sorting methods, so we can ignore the order from Elasticsearch
          %w| asc desc order_by |.each do |name|
            define_method name do |*args|
              raise "TODO - not supported yet"
            end
          end
        end

        module Callbacks
          def self.included(base)
            base.after_create  { |document| document.__elasticsearch__.index_document  }
            base.after_update  { |document| document.__elasticsearch__.update_document }
            base.after_destroy { |document| document.__elasticsearch__.delete_document }
          end
        end

        module Importing

          def __find_in_batches(options={}, &block)
            # Use 1/4 or read provision
            read_provision = self.dynamo_db_table.table_schema[:provisioned_throughput][:read_capacity_units]
            raise "read_provision not set for class!" unless read_provision
            default_batch_size = (read_provision / 2.0).floor
            batch_size = options[:batch_size] || default_batch_size
            puts "Indexing via scan with batch size of #{batch_size}..."

            # :consumed_capacity
            scan_idx = 0
            results_hash = {}
            while scan_idx == 0 || (results_hash && results_hash[:last_evaluated_key])
              puts "Batch iteration #{scan_idx+1}..."
              scan_options = {
                batch: batch_size,
                manual_batching: true,
                return_consumed_capacity: :total
              }
              scan_options.merge!(exclusive_start_key: results_hash[:last_evaluated_key]) if results_hash[:last_evaluated_key]
              scan_options.merge!(scan_filter: options[:scan_filter]) if options[:scan_filter]
              results_hash = self.scan(scan_options)

              unless results_hash[:results].blank?
                puts "Indexing #{results_hash[:results].size} results..."
                yield results_hash[:results]
              end

              # If more results to scan, sleep to throttle...
              #   Local Dynamo is not returning consumed_capacity 2014-01-12
              if results_hash[:last_evaluated_key] && results_hash[:consumed_capacity]
                # try to keep read usage under 50% of read_provision
                sleep_time = results_hash[:consumed_capacity][:capacity_units].to_f / (read_provision / 2.0)
                puts "Sleeping for #{sleep_time}..."
                sleep(sleep_time)
              end
              
              scan_idx += 1
            end
          end

          def __transform
            lambda { |a| { index: {
              _id: a.id,
              data: a.as_indexed_json
            } } }
          end

        end

      end

    end
  end
end
