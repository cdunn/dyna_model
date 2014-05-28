# TODO: S3 key schema that allows for timestamp sorting
#
# Persist DynaModel records for a particular model to S3 for extra backup.
#
# The DynamoDB backup model (using EMR to read and write to S3 is not incremental and quickly takes way too long
#   and defeats the purpose of a backup)
#
# This is not intended to be used for models with high frequency writes but as a way to incrementally backup models
# that contain mission critical data (although S3 backups are not guarenteed to be durable since you probably want to delay
# the S3 write to a backround task).
#
module DynaModel
  module Extensions
    module S3Backup
      extend ActiveSupport::Concern

      included do
        after_save :backup_dyna_model_record_to_s3
      end

      def backup_dyna_model_record_to_s3
        return if DynaModel::Config.s3_backup_extension_enable_development && DynaModel::Config.s3_backup_extension_development_environments.include?(Rails.env)
        if self.class.dyna_model_s3_backup_config
          if self.class.dyna_model_s3_backup_config[:after_save]
            self.class.dyna_model_s3_backup_config[:after_save].call(self)
          else
            self.write_dyna_model_s3_backup!
          end
        end
      end

      def write_dyna_model_s3_backup!
        self.class.dyna_model_s3_backup_bucket.objects[File.join(self.class.dyna_model_s3_backup_config[:prefix], "#{self.dynamo_db_guid}.json")].write(self.to_dyna_model_s3_backup_json)
      end

      def to_dyna_model_s3_backup_json
        ActiveSupport::JSON.encode({
          class: self.class.to_s,
          attributes: self.attributes
        })
      end

      module ClassMethods

        def dyna_model_s3_backup(options={})
          raise "DynaModel::Extensions::S3Backup requires a bucket." unless options[:bucket]
          options[:prefix] ||= "#{self.to_s.underscore.pluralize}-#{Rails.env}"
          #options[:after_save] = lambda { |obj| ... }
          (@@dyna_model_s3_backup_config ||= {})[self.to_s] = options.dup
        end

        def dyna_model_s3_backup_client
          Thread.current[:dyna_model_s3_backup_client] ||= AWS::S3.new
        end

        def dyna_model_s3_backup_config
          (@@dyna_model_s3_backup_config ||= {})[self.to_s]
        end

        def dyna_model_s3_backup_bucket
          self.dyna_model_s3_backup_client.buckets[self.dyna_model_s3_backup_config[:bucket]]
        end

        def enable_dyna_model_s3_backup_versioning!
          self.dyna_model_s3_backup_bucket.enable_versioning
        end

        def suspend_dyna_model_s3_backup_versioning!
          self.dyna_model_s3_backup_bucket.suspend_versioning
        end

        def create_dyna_model_s3_backup_bucket!
          self.dyna_model_s3_backup_client.buckets.create(self.dyna_model_s3_backup_config[:bucket])
        end

        # Helper for sidekiq/etc delay method on class
        def dyna_model_s3_backup_object(guid)
          self.read_guid(guid).try(:write_dyna_model_s3_backup!)
        end

        # TODO: improve for high scale... ability to resume
        def import_from_dyna_model_s3_backup
          self.dyna_model_s3_backup_bucket.objects.with_prefix(self.dyna_model_s3_backup_config[:prefix]).each_batch do |batch|
            batch.each do |item|
              puts "Found #{item.key}"
              obj_json = ActiveSupport::JSON.decode(item.read)
              obj = obj_json["class"].constantize.new
              obj.attributes = obj_json["attributes"]
              if obj.save
                puts "Saved #{obj.dynamo_db_guid}."
              else
                puts "Failed to save #{obj.dynamo_db_guid}. #{obj.errors.full_messages.to_sentence}"
              end
            end
          end
        end

      end

    end
  end
end
