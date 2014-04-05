require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe "DynaModel Callbacks" do

  before do
    Callbacker.create_table
  end

  it 'should trigger create callbacks' do
    callbacker = Callbacker.create(id: 123)
    callbacker.persisted?.should be_true
    callbacker.before_create_method_attr.should be_true
    callbacker.before_create_block_attr.should be_true
    callbacker.before_validation_on_create_method_attr.should be_false
  end

  it 'should trigger save callbacks' do
    callbacker = Callbacker.create(id: 123)
    callbacker.persisted?.should be_true
    callbacker.before_save_counter.should == 1
    callbacker.before_validation_on_create_method_attr = true
    callbacker.touch!
    callbacker.before_save_counter.should == 2
  end

  it 'should trigger update callbacks' do
    callbacker = Callbacker.create(id: 123)
    callbacker.persisted?.should be_true
    callbacker.before_update_counter.should == 0
    callbacker.before_validation_on_create_method_attr = true
    callbacker.touch!
    callbacker.before_update_counter.should == 1
  end

end
