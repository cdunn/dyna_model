require 'carrierwave'
require 'carrierwave/validations/active_model'

module CarrierWave
  module DynaModel
    include CarrierWave::Mount

    def mount_uploader(column, uploader, options={}, &block)
      options[:mount_on] ||= "#{column}_identifier"
      string_attr options[:mount_on].to_sym

      super

      alias_method :read_uploader, :[]
      alias_method :write_uploader, :[]=
      public :read_uploader
      public :write_uploader

      include CarrierWave::Validations::ActiveModel

      validates_integrity_of  column if uploader_option(column.to_sym, :validate_integrity)
      validates_processing_of column if uploader_option(column.to_sym, :validate_processing)
      
      after_save "store_#{column}!".to_sym
      before_save "write_#{column}_identifier".to_sym
      after_destroy "remove_#{column}!".to_sym

    end

  end
end

DynaModel::Document::ClassMethods.send(:include, CarrierWave::DynaModel)
