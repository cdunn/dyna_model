# TODO: optimistic locking?

module DynaModel
  module Persistence
    extend ActiveSupport::Concern

    def _guid
      raise self.inspect
    end

    private
    def populate_id
      #@_id = UUIDTools::UUID.random_create.to_s.downcase
    end

    private
    def dynamo_item_key_values
      key_values = { hash_value: self[self.class.hash_key[:attribute_name]] }
      key_values.merge!(range_value: self[self.class.range_key[:attribute_name]]) if self.class.range_key
      key_values
    end

    private
    def dynamo_db_table
      self.class.dynamo_db_table(shard)
    end

    private
    def create_storage
      self.class.dynamo_db_table.write(serialize_attributes)
    end

    private
    def update_storage
      # Only enumerating dirty (i.e. changed) attributes.  Empty
      # (nil and empty set) values are deleted, the others are replaced.
      attr_updates = {}
      changed.each do |attr_name|
        attribute = self.class.attribute_for(attr_name)
        value = serialize_attribute(attribute, @_data[attr_name])
        if value.nil? or value == []
          attr_updates[attr_name] = nil
        else
          attr_updates[attr_name] = value
        end
      end

      self.class.dynamo_db_table.write(attr_updates, {
        update_item: dynamo_item_key_values,
        shard_name: self.shard
      })
    end

    private
    def delete_storage
      self.class.dynamo_db_table.delete_item(
        delete_item: dynamo_item_key_values,
        shard_name: self.shard
      )
    end

    private
    def deserialize_item_data data
      data.inject({}) do |hash,(attr_name,value)|
        if attribute = self.class.attributes[attr_name]
          hash[attr_name] = value.is_a?(Set) ?
            value.map{|v| attribute.deserialize(v) } :
            attribute.deserialize(value)
        end
        hash
      end
    end

    module ClassMethods
    end

  end
end
