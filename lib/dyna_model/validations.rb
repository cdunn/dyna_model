module AWS
  module Record
    module AbstractBase

      # OVERRIDE
      # https://github.com/aws/aws-sdk-ruby/blob/master/lib/aws/record/abstract_base.rb#L20
      # Disable aws-sdk validations in favor of ActiveModel::Validations
      def self.extended base
        base.send(:extend, ClassMethods)
        base.send(:include, InstanceMethods)
        base.send(:include, DirtyTracking)
        #base.send(:extend, Validations)

        # these 3 modules are for rails 3+ active model compatability
        base.send(:extend, Naming)
        base.send(:include, Naming)
        base.send(:include, Conversion)
      end

    end
  end
end

module DynaModel
  module Validations
    extend ActiveSupport::Concern
    include ActiveModel::Validations
    include ActiveModel::Validations::Callbacks

    module ClassMethods
    end
  end
end
