require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe "DynaModel::Attributes" do

  before do
    User.create_table
    @user = User.new
  end

  it 'should add attributes' do
    @user.hashy.should be_nil
    @user.is_dude.should be_nil
    @user.born.should be_nil
  end

  it 'should read default attribute' do
    @user.ranger.should == 2
    @user.name.should == "dude"
  end

  it 'should read and write StringAttr' do
    @user.hashy.should be_nil
    @user.hashy = "hash"
    @user.hashy.should == "hash"
  end

  it 'should read and write IntegerAttr' do
    @user.intous.should be_nil
    @user.intous = 1
    @user.intous.should == 1
  end

  it 'should read and write BooleanAttr' do
    @user.is_dude = true
    @user.is_dude.should be_true
    @user.is_dude = false
    @user.is_dude.should be_false
  end

  it 'should read and write DateTimeAttr' do
    birthday = 27.years.ago.to_date
    @user.born = birthday
    @user.born.should == birthday
  end

  it 'should read and write SerializedAttr' do
    @user.cereal.should be_nil
    @user.cereal = { hi: "yo" }
    @user.cereal.should == { hi: "yo" }
    @user.cereal = [1,2,"3"]
    @user.cereal.should == [1,2,"3"]
  end

end
