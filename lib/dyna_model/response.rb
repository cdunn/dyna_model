module DynaModel
  class Response

    def initialize(response)
      raise ArgumentError, "response should be an AWS::Core::Response" unless response.is_a?(AWS::Core::Response)
      @raw_response = response
    end

    #def values_from_response_hash(options = {})
      #@raw_response.inject({}) do |h, (key, value_hash)|
        #h.update(key => value_hash.to_a.last)
      #end
    #end

    def count
      @raw_response[:count]
    end

    def last_evaluated_key
      @raw_response[:last_evaluated_key]
    end

    def more_results?
      @raw_response.has_key?(:last_evaluated_key)
    end

    def self.strip_attr_types(hash)
      attrs = {}
      hash.each_pair do |k,v|
        attrs[k] = v.values.first
      end
      attrs
    end

  end
end
