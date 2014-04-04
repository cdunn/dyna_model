require "aws-sdk"
require "rails"
require "active_support"
require 'active_support/concern'
require 'active_model'
require "dyna_model/aws/record/attributes/serialized_attr"
require "dyna_model/version"
require "dyna_model/config"
require "dyna_model/attributes"
require "dyna_model/schema"
require "dyna_model/persistence"
require "dyna_model/table"
require "dyna_model/query"
require "dyna_model/response"
require "dyna_model/extensions/symbol"
require "dyna_model/document"

module DynaModel

  extend self

  def configure
    block_given? ? yield(DynaModel::Config) : DynaModel::Config
  end
  alias :config :configure

  def logger
    DynaModel::Config.logger
  end

end
