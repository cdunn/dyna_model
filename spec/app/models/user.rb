class User

  include DynaModel::Document
  
  string_attr :hashy
  integer_attr :ranger
  serialized_attr :cereal
  string_attr :name
  timestamps

  hash_key :hashy
  range_key :ranger

  set_shard_name "usery"

  local_secondary_index :name
  global_secondary_index(:name_index, { hash_key: :name, projection: [:name] })

  read_provision 4
  write_provision 4
  guid_delimiter "!"

end
