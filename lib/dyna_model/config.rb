# encoding: utf-8
require "uri"
require "dyna_model/config/options"

# Shamelessly stolen from Dynamoid
module DynaModel

  # Contains all the basic configuration information required for Dynamoid: both sensible defaults and required fields.
  module Config
    extend self
    extend Options

    # All the default options.
    option :logger, default: defined?(Rails)
    option :read_provision, default: 50
    option :write_provision, default: 10
    # TODO - default adapter client based on config
    #option :namespace, :default => defined?(Rails) ? "#{Rails.application.class.parent_name}_#{Rails.env}" : ""
    option :endpoint, default: nil # default
    option :region, default: 'us-west-2'
    option :default_guid_delimiter, default: ":"
    option :namespace, default: ""

    option :lock_extension_read_provision, default: 10
    option :lock_extension_write_provision, default: 4
    option :lock_extension_shard_name, default: "lock"
    
    option :s3_backup_extension_enable_development, default: false
    option :s3_backup_extension_development_environments, default: %w(development test)

    # The default logger: either the Rails logger or just stdout.
    def default_logger
      defined?(Rails) && Rails.respond_to?(:logger) ? Rails.logger : ::Logger.new($stdout)
    end

    # Returns the assigned logger instance.
    def logger
      @logger ||= default_logger
    end

    # If you want to, set the logger manually to any output you'd like. Or pass false or nil to disable logging entirely.
    def logger=(logger)
      case logger
      when false, nil then @logger = nil
      when true then @logger = default_logger
      else
        @logger = logger if logger.respond_to?(:info)
      end
    end

  end
end
