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
    option :logger, :default => defined?(Rails)
    option :read_provision, :default => 50
    option :write_provision, :default => 10
    # TODO - default adapter client based on config
    #option :namespace, :default => defined?(Rails) ? "#{Rails.application.class.parent_name}_#{Rails.env}" : ""
    option :endpoint, :default => 'dynamodb.us-west-2.amazonaws.com'
    option :port, :default => 443
    option :use_ssl, :default => true
    option :default_guid_delimiter, :default => ":"

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
