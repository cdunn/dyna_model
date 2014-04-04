require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe "DynaModel::Attributes" do

  before do
    User.create_table
    @user = User.new
  end

  it 'test' do
    @user.hashy = "hash"
    @user.ranger = 1
    @user.cereal = {hi: 'yo'}
    @user.name = "c"
    @user.save.should be_true
    User.read("hash", 1).should_not be_nil
  end

end
