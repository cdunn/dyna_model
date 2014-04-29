module AWS
  module Record
    module Attributes
      class SerializedAttr < BaseAttr

        # OVERRIDE
        # https://github.com/aws/aws-sdk-ruby/blob/master/lib/aws/record/attributes.rb#L372
        # Allow Time instead of just DateTime
        def self.serialize datetime, options = {}
          unless datetime.is_a?(DateTime) || datetime.is_a?(Time)
            msg = "expected a DateTime value, got #{datetime.class}"
            raise ArgumentError, msg
          end
          datetime.strftime('%Y-%m-%dT%H:%M:%S%Z')
        end

      end
    end
  end
end
