class Callbacker

  include DynaModel::Document
  
  string_attr :id
  boolean_attr :before_create_block_attr, default_value: false
  boolean_attr :before_create_method_attr, default_value: false
  boolean_attr :before_validation_on_create_method_attr, default_value: false
  validates_inclusion_of :before_validation_on_create_method_attr, in: [true]
  integer_attr :before_save_counter, default_value: 0
  integer_attr :before_update_counter, default_value: 0
  timestamps

  hash_key :id

  read_provision 2
  write_provision 8

  before_create :before_create_method
  before_create do
    self.before_create_block_attr = true
  end
  before_validation :before_validation_on_create_method, on: :create
  after_create :after_create_change_before_validation

  before_save do
    self.before_save_counter += 1
  end

  before_update do
    self.before_update_counter += 1
  end

  def before_create_method
    self.before_create_method_attr = true
  end

  def before_validation_on_create_method
    self.before_validation_on_create_method_attr = true
  end

  def after_create_change_before_validation
    self.before_validation_on_create_method_attr = false
  end

end
