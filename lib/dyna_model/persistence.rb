module DynaModel
  module Persistence
    extend ActiveSupport::Concern

    private
    def populate_id
      #raise self.class.hash_key.inspect
      #@_id = UUIDTools::UUID.random_create.to_s.downcase
    end

    private
    def dynamo_db_item
      raise "dynamo_db_item"
      #dynamo_db_table.items[hash_key]
      #dynamo_db_table.items.query({
        #hash_value: hash_key
      #})
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
      #dynamo_db_item.attributes.update(opt_lock_conditions) do |u|
        #changed.each do |attr_name|
          #attribute = self.class.attribute_for(attr_name)
          #value = serialize_attribute(attribute, @_data[attr_name])
          #if value.nil? or value == []
            #u.delete(attr_name)
          #else
            #u.set(attr_name => value)
          #end
        #end
      #end
    end

    private
    def delete_storage
      #dynamo_db_item.delete(opt_lock_conditions)
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
