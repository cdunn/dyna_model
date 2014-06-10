# TODO: optimistic locking?

module DynaModel
  module Persistence
    extend ActiveSupport::Concern

    private
    def populate_id
      #@_id = UUIDTools::UUID.random_create.to_s.downcase
    end

    private
    def dynamo_db_table
      self.class.dynamo_db_table(shard)
    end

    private
    def create_storage(options={})
      run_callbacks :save do
        run_callbacks :create do
          self.class.dynamo_db_table.write(serialize_attributes, options.merge(shard_name: self.shard))
        end
      end
    end

    private
    def update_storage(options={})
      # Only enumerating dirty (i.e. changed) attributes.  Empty
      # (nil and empty set) values are deleted, the others are replaced.
      run_callbacks :save do
        run_callbacks :update do
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

          self.class.dynamo_db_table.write(attr_updates, options.merge({
            update_item: dynamo_db_item_key_values,
            shard_name: self.shard
          }))
        end
      end
    end

    private
    def delete_storage(options={})
      run_callbacks :destroy do
        self.class.dynamo_db_table.delete_item(options.merge(
          delete_item: dynamo_db_item_key_values,
          shard_name: self.shard
        ))
      end
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
