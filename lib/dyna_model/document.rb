module DynaModel
  module Document

    MAX_ITEM_SIZE = 65_536

    extend ActiveSupport::Concern

    included do
      #class_attribute :options, :read_only_attributes, :base_class
      #self.options = {}
      #self.read_only_attributes = []
      #self.base_class = self

      #Dynamoid::Config.included_models << self
      extend AWS::Record::AbstractBase
      extend ActiveModel::Translation
      extend ActiveModel::Callbacks

      define_model_callbacks :create, :save, :destroy, :initialize, :update

      #before_create :set_created_at
      #before_save :set_updated_at
      #after_initialize :set_type
      #puts "included"
    end

    #include ActiveModel::AttributeMethods
    include ActiveModel::Conversion
    include ActiveModel::MassAssignmentSecurity if defined?(ActiveModel::MassAssignmentSecurity)
    include ActiveModel::Naming
    include ActiveModel::Observing if defined?(ActiveModel::Observing)
    include ActiveModel::Serializers::JSON
    include ActiveModel::Serializers::Xml
    
    include DynaModel::Attributes
    include DynaModel::Schema

    #include Dynamoid::Fields
    #include Dynamoid::Indexes
    #include Dynamoid::Persistence
    #include Dynamoid::Finders
    #include Dynamoid::Associations
    #include Dynamoid::Criteria
    #include Dynamoid::Validations
    #include Dynamoid::IdentityMap
    #include Dynamoid::Dirty

    # @api private
    def populate_id
      raise self.class.hash_key.inspect
      #@_id = UUIDTools::UUID.random_create.to_s.downcase
    end

    module ClassMethods

    end
  end
end
