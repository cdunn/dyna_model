class Validez

  include DynaModel::Document
  
  string_attr :key
  string_attr :ranger

  string_attr :body
  validates_presence_of :body

  boolean_attr :bool_party, default_value: false
  validates :bool_party, inclusion: { in: [true, false] }

  integer_attr :inteater
  validates :inteater, numericality: { greater_than: 1, less_than: 4 }

  string_attr :superhero

  validate :check_superhero, if: lambda { |v| v.inteater == 3 }

  hash_key :key
  range_key :ranger

  read_provision 4
  write_provision 4

  def check_superhero
    unless self.superhero == "batman"
      self.errors.add(:superhero, 'should be batman')
    end
  end

end
