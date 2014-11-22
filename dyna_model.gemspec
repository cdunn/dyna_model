# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'dyna_model/version'

Gem::Specification.new do |spec|
  spec.name          = "dyna_model"
  spec.version       = DynaModel::VERSION
  spec.authors       = ["Cary Dunn"]
  spec.email         = ["cary.dunn@gmail.com"]
  spec.summary       = %q{DyanmoDB ORM on AWS::Record}
  spec.description   = %q{DyanmoDB ORM on AWS::Record}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "mocha"

  spec.add_dependency 'activesupport', '>= 4.0.0'
  spec.add_dependency 'activemodel', '>= 4.0.0'
  spec.add_dependency 'rails', '>= 4.0.0'
  spec.add_dependency 'aws-sdk', '>= 2.0.10.pre'
  spec.add_dependency 'aws-sdk-v1', '>= 1.39.0'
end
  
