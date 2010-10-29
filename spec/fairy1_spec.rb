# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'rspec'

require 'fairy'
require 'fairy/controller'

describe Fairy do
  before :all do
    @fairy = Fairy::Fairy.new
  end

  # initialize
  it 'should create new controller' do
    @fairy.controller.kind_of?(Fairy::Controller).should be_true
    @fairy.controller.deep_space.status.should == :SERVICING
  end

  # abort
  it 'should destroy the controller' do
    @fairy.abort
    sleep(3)
    @fairy.controller.deep_space.status.should == :SERVICE_STOP
  end
end


