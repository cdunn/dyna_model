require 'rake'

module DynaModel
  module Tasks
    extend self
    def included_models
      dir = ENV['DIR'].to_s != '' ? ENV['DIR'] : Rails.root.join("app/models")
      DynaModel::Config.logger.info "Loading models from: #{dir}"
      included = []
      Dir.glob(File.join("#{dir}/**/*.rb")).each do |path|
        model_filename = path[/#{Regexp.escape(dir.to_s)}\/([^\.]+).rb/, 1]
        next if model_filename.match(/^concerns\//i) # Skip concerns/ folder

        begin
          klass = model_filename.camelize.constantize
        rescue NameError
          require(path) ? retry : raise
        rescue LoadError => e
          # Try non-namespaced class name instead...
          klass = model_filename.camelize.split("::").last.constantize
        end

        # Skip if the class doesn't have DynaModel integration
        next unless klass.respond_to?(:dynamo_db_table)

        included << klass
      end
      included
    end
  end
end

namespace :ddb do
  desc 'Create a DynamoDB table'
  task :create => :environment do
    raise "expected usage: rake ddb:create CLASS=User" unless ENV['CLASS']
    options = {}
    options.merge!(shard_name: ENV['SHARD']) if ENV['SHARD']
    if ENV["CLASS"] == "all"
      DynaModel::Tasks.included_models.each do |klass|
        DynaModel::Config.logger.info "Creating table for #{klass}..."
        begin
          klass.create_table(options)
        rescue Exception => e
          DynaModel::Config.logger.info "Could not create table! #{e.inspect}"
        end
      end
    else
      ENV['CLASS'].constantize.create_table(options)
    end
  end

  desc 'Resize a DynamoDB table read/write provision'
  task :resize => :environment do
    raise "expected usage: rake ddb:resize CLASS=User" unless ENV['CLASS']
    options = {}
    options.merge!(shard_name: ENV['SHARD']) if ENV['SHARD']
    options.merge!(read_capacity_units: ENV['READ'].to_i) if ENV['READ']
    options.merge!(write_capacity_units: ENV['WRITE'].to_i) if ENV['WRITE']
    ENV['CLASS'].constantize.resize_table(options)
  end

  desc 'Destroy a DynamoDB table'
  task :destroy => :environment do
    raise "expected usage: rake ddb:destroy CLASS=User" unless ENV['CLASS']
    options = {}
    options.merge!(shard_name: ENV['SHARD']) if ENV['SHARD']
    if ENV["CLASS"] == "all"
      DynaModel::Tasks.included_models.each do |klass|
        DynaModel::Config.logger.info "Destroying table for #{klass}..."
        begin
          klass.delete_table(options)
        rescue Exception => e
          DynaModel::Config.logger.info "Could not create table! #{e.inspect}"
        end
      end
    else
      ENV['CLASS'].constantize.delete_table(options)
    end
  end
end
