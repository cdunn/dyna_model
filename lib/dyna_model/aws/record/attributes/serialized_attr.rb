module AWS
  module Record
    module Attributes
      class SerializedAttr < BaseAttr

        def self.type_cast raw_value, options = {}
          case raw_value
          when nil      then nil
          when ''       then nil
          when String # assume binary
            begin
              Marshal.load(raw_value)
            rescue
              raw_value
            end
          else # object to serialize
            raw_value
          end
        end

        def self.serialize obj, options = {}
          AWS::DynamoDB::Binary.new(Marshal.dump(obj))
        end

        # @api private
        def self.allow_set?
          false
        end

      end
    end
  end
end
