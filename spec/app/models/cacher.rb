class Cacher

  include DynaModel::Document
  
  string_attr :key
  string_attr :body
  timestamps

  hash_key :key

  read_provision 4
  write_provision 4

end
