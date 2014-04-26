require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe "DynaModel::Persistence" do

  before do
    User.delete_table
    User.create_table
    @user = User.new
    @user_attrs = {
      hashy: "hash",
      ranger: 3,
      name: "Kate",
      intous: 100,
      is_dude: false,
      born: 28.year.ago.to_date,
      cereal: { frosted: "mini wheats" }
    }
  end

  it 'should save assigned attributes' do
    @user_attrs.each_pair do |k,v|
      @user.send("#{k}=", v)
    end
    @user.save.should be_true
    User.read("hash", 3).attributes.each_pair do |k,v|
      next if %w(created_at updated_at).include?(k)
      @user_attrs[k.to_sym].should == v
    end
  end

  it 'should save assigned attributes by mass assign' do
    user = User.new(@user_attrs)
    user.save.should be_true
    User.read("hash", 3).attributes.each_pair do |k,v|
      next if %w(created_at updated_at).include?(k)
      @user_attrs[k.to_sym].should == v
    end
  end

  it 'should update an attribute' do
    user = User.new(@user_attrs)
    user.save
    user.intous = 101
    user.save
    User.read("hash", 3).intous.should == 101
    user.name = "Katie"
    user.save
    User.read("hash", 3).name.should == "Katie"
    user.cereal = @user_attrs[:cereal].merge(cheerios: "regular")
    user.save
    User.read("hash", 3).cereal.should == {
      frosted: "mini wheats",
      cheerios: "regular"
    }
    user.cereal_will_change!
    user.cereal.merge!(cheerios: "honey nut")
    user.save
    User.read("hash", 3).cereal.should == {
      frosted: "mini wheats",
      cheerios: "honey nut"
    }
  end

  it 'should update attributes by mass assign' do
    user = User.new(@user_attrs)
    user.save
    user.update_attributes({name: "Katie", cereal: {granola: "with blueberries"}})
    updated_user = User.read("hash", 3)
    updated_user.name.should == "Katie"
    updated_user.cereal.should == {granola: "with blueberries"}
  end

  it 'should destroy record' do
    User.read("hash", 3).should be_nil
    user = User.new(@user_attrs)
    user.save
    User.read("hash", 3).should_not be_nil
    user.destroy.should be_true
    User.read("hash", 3).should be_nil
  end

  it 'should respect :expected in a save' do
    user = User.new(@user_attrs)

    expect {
      user.save
    }.not_to raise_error

    expect {
      user.save(expected: {:name.eq => "wrongname"})
    }.to raise_error(AWS::DynamoDB::Errors::ConditionalCheckFailedException)

    user.save(expected: {:name.eq => "Kate"}, return_values: :all_old).should be_a AWS::Core::Response
  end

  it 'should respect :expected in a destroy' do
    user = User.new(@user_attrs)
    user.save

    expect {
      user.delete(expected: {:name.eq => "wrongname"})
    }.to raise_error(AWS::DynamoDB::Errors::ConditionalCheckFailedException)

    expect {
      user.delete(expected: {:name.eq => "Kate"})
    }.not_to raise_error
  end

end
