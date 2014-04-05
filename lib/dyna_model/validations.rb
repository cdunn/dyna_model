module DynaModel
  module Validations
    extend ActiveSupport::Concern

    module ClassMethods

      def before_validation(*args, &block)
        options = args.last
        validation_context = nil
        if options.is_a?(Hash) && %w(create update).include?(options[:on].to_s)
          validation_context = options[:on].to_s
        else
          validation_context = "save"
        end
        self.send("before_validation_on_#{validation_context}", *args, &block)
      end

      def after_validation(*args, &block)
        options = args.last
        validation_context = nil
        if options.is_a?(Hash) && %w(create update).include?(options[:on].to_s)
          validation_context = options[:on].to_s
        else
          validation_context = "save"
        end
        self.send("after_validation_on_#{validation_context}", *args, &block)
      end

    end

  end
end
