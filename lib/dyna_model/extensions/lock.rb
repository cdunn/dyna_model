module DynaModel
  module Extensions
    class Lock

      class LockNotAcquired < StandardError
      end

      HOST = `hostname`.strip

      include DynaModel::Document

      string_attr :lock_name
      datetime_attr :locked_at
      datetime_attr :expires_at
      string_attr :locked_by

      hash_key :lock_name

      set_shard_name DynaModel::Config.lock_extension_shard_name

      read_provision DynaModel::Config.lock_extension_read_provision
      write_provision DynaModel::Config.lock_extension_write_provision

      def self.locked_by
        "#{HOST}:#{Process.pid}"
      end

      def self.lock(lock_name, options={}, &block)
        lock_acquired = false
        if lock_obj_acquired = self.acquire(lock_name, options)
          lock_acquired = true
          if block
            begin
              result = (block.arity == 1) ? block.call(lock_obj_acquired) : block.call
            ensure
              release(lock_name) if lock_acquired
            end
          end
          result
        end
      end

      def self.acquire(lock_name, options={})
        options[:acquisition_timeout] ||= 15
        options[:expires_in] ||= 10

        retries = 0
        begin
          Timeout::timeout(options[:acquisition_timeout]) do
            begin
              locked_at = DateTime.now
              expires_at = locked_at + options[:expires_in].seconds
              lock_obj = self.new(
                lock_name: lock_name,
                locked_at: locked_at,
                expires_at: expires_at,
                locked_by: self.locked_by
              )
              if lock_obj.save(expected: {
                :locked_by.null => "",
                :expires_at.le => locked_at
              }, conditional_operator: :or)
                DynaModel::Config.logger.info "Acquired lock '#{lock_name}'"
                lock_obj
              else
                raise "Error acquiring lock: #{lock_obj.errors.full_messages.to_sentence}"
              end
            rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
              DynaModel::Config.logger.info "Lock condition failed for '#{lock_name}'. Retrying..."
              sleep((2 ** retries) * 0.05)
              retries += 1
              retry
            end
          end
        rescue Timeout::Error
          raise LockNotAcquired, "Could not acquire lock after #{options[:acquisition_timeout]} seconds"
        end
      end

      def self.release(lock_name, options={})
        lock_obj = self.new(
          lock_name: lock_name,
          locked_at: nil,
          expires_at: nil,
          locked_by: nil
        )

        if lock_obj.save(expected: {
          :locked_by.eq => self.locked_by
        })
          DynaModel::Config.logger.info "Released lock '#{lock_name}'"
          true
        else
          raise "Error releasing lock: #{lock_obj.errors.full_messages.to_sentence}"
        end
      rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException => e
        DynaModel::Config.logger.info "Condition failed to release lock '#{lock_name}'"
      end

      def self.extend(lock_name, extension_time=5.seconds)
        locked_at = DateTime.now
        expires_at = locked_at + extension_time

        lock_obj = self.new(
          lock_name: lock_name,
          locked_at: locked_at,
          expires_at: expires_at,
          locked_by: self.locked_by
        )

        if lock_obj.save(expected: {
          :locked_by.eq => self.locked_by
        })
          DynaModel::Config.logger.info "Extended lock '#{lock_name}' for #{extension_time} seconds"
          true
        else
          raise "Error extending lock: #{lock_obj.errors.full_messages.to_sentence}"
        end
      rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException => e
        DynaModel::Config.logger.info "Condition failed to extend lock '#{lock_name}'"
      end

    end
  end
end
