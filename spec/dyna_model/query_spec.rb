require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe "DynaModel::Query" do

  before do
    User.create_table
    Cacher.create_table
    @user_attrs = {
      hashy: "Dunn",
      ranger: 1,
      name: "Kate",
      born: 28.year.ago.to_date,
      intous: 1
    }

    @user2_attrs = {
      hashy: "Dunn",
      ranger: 2,
      name: "Cary",
      born: 27.year.ago.to_date,
      intous: 2
    }

    @user3_attrs = {
      hashy: "Cary",
      ranger: 3,
      name: "Dunn",
      born: 100.year.ago.to_date,
      intous: 3
    }
  end

  it 'should read_guid' do
    User.read_guid("Dunn!1").should be_nil
    @user = User.create(@user_attrs)
    User.read_guid("Dunn!1").should_not be_nil
    Cacher.read_guid("123").should be_nil
    @cacher = Cacher.create(key: "123", body: "content")
    Cacher.read_guid("123").should_not be_nil
  end

  it 'should read by hash and range values' do
    User.read("Dunn", 1).should be_nil
    @user = User.create(@user_attrs)
    User.read("Dunn", 1).should_not be_nil
    Cacher.read("123").should be_nil
    @cacher = Cacher.create(key: "123", body: "content")
    Cacher.read("123").should_not be_nil
  end

  it 'should read_range' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    User.read_range("Dunn").length.should == 2
    User.read_range("Dunn", order: :asc).first.name.should == "Kate"
  end

  it 'should read_range with :select' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    user = User.read_range("Dunn", select: [:name]).first
    user.name.should be_present
    expect {
      user.born
    }.to raise_error
  end

  it 'should read_range with :limit' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    User.read_range("Dunn").length.should == 2
    User.read_range("Dunn", limit: 1).length.should == 1
  end

  it 'should read_range with local secondary range key' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    users = User.read_range("Dunn", range: {:name.eq => "Cary"})
    users.length.should == 1
    users.first.name.should == "Cary"
  end

  it 'should read_range with :global_secondary_index' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    users = User.read_range("Cary", global_secondary_index: :name_index)
    users.length.should == 1
    users.first.name.should == "Cary"
  end

  it 'should read_range with int :global_secondary_index' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    users = User.read_range(2, global_secondary_index: :intous_index)
    users.length.should == 1
    users.first.name.should == "Cary"
  end

  it 'should read_range with :query_filter' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    users = User.read_range("Dunn", query_filter: {:name.eq => "Cary"})
    users.length.should == 1
    users.first.name.should == "Cary"
    users = User.read_range("Dunn", query_filter: {:name.in => ["Cary"]})
    users.length.should == 1
    users.first.name.should == "Cary"
  end

  it 'should read_first' do
    @user = User.create(@user_attrs)
    User.read_first("Dunn", order: :asc).name.should == "Kate"
  end

  it 'should count_range' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    User.count_range("Dunn").should == 2
  end

  it 'should read_multiple' do
    @cacher1 = Cacher.create(key: "123", body: "content")
    @cacher2 = Cacher.create(key: "234", body: "content1")
    multi = Cacher.read_multiple([@cacher1.key, @cacher2.key])
    multi[@cacher1.key].should_not be_nil
    multi[@cacher2.key].should_not be_nil
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    multi = User.read_multiple([@user.dynamo_db_guid, @user2.dynamo_db_guid])
    multi[@user.hashy].should_not be_nil
    multi[@user.hashy][@user.ranger].should_not be_nil
    multi[@user2.hashy].should_not be_nil
    multi[@user2.hashy][@user2.ranger].should_not be_nil
  end

  it 'should scan' do
    User.scan.length.should == 0
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    User.scan.length.should == 2
    User.scan(scan_filter: { :name.begins_with => "C" }).length.should == 1
    User.scan(scan_filter: { :intous.between => 1..2 }).length.should == 2
  end

  it 'should paginate' do
    @user = User.create(@user_attrs)
    @user2 = User.create(@user2_attrs)
    @user3 = User.create(@user3_attrs)
    expect(User.scan(batch: 1).size).to eq 3
    expect(User.scan(limit: 1).size).to eq 1
  end

end
