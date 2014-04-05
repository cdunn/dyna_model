require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

describe "DynaModel Validations" do

  before do
    Validez.create_table
    @valid_params = {
      key: "key",
      ranger: "ranger",
      body: "so fine",
      bool_party: true,
      inteater: 2
    }
  end

  it 'should create from valid params' do
    validez = Validez.create(@valid_params)
    validez.persisted?.should be_true
  end

  it 'should enforce hash_key presence' do
    validez = Validez.create(@valid_params.merge(key: nil))
    validez.persisted?.should be_false
    validez.errors.messages[:key].should be_present
  end

  it 'should enforce range_key presence' do
    validez = Validez.create(@valid_params.merge(ranger: nil))
    validez.persisted?.should be_false
    validez.errors.messages[:ranger].should be_present
  end

  it 'should enforce boolean presence' do
    validez = Validez.create(@valid_params.merge(bool_party: nil))
    validez.persisted?.should be_false
    validez.errors.messages[:bool_party].should be_present
  end

  it 'should enforce integer numericality' do
    validez = Validez.create(@valid_params.merge(inteater: 'asdf'))
    validez.persisted?.should be_false
    validez.errors.messages[:inteater].should be_present

    validez = Validez.create(@valid_params.merge(inteater: 9))
    validez.persisted?.should be_false
    validez.errors.messages[:inteater].should be_present

    validez = Validez.create(@valid_params.merge(inteater: 2))
    validez.persisted?.should be_true
  end

  it 'should enforce validation via method' do
    validez = Validez.create(@valid_params.merge(inteater: 3))
    validez.persisted?.should be_false
    validez.errors.messages[:superhero].should be_present

    validez = Validez.create(@valid_params.merge(inteater: 3, superhero: "batman"))
    validez.persisted?.should be_true
  end

end
