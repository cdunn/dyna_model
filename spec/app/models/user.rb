class User

  include DynaModel::Document
  
  string_attr :hashy
  integer_attr :ranger, default_value: 2
  string_attr :name, default_value: lambda { "dude" }
  integer_attr :intous
  boolean_attr :is_dude
  datetime_attr :born
  serialized_attr :cereal
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
