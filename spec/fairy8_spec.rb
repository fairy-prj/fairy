# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'rspec'
require 'yaml'

require 'fairy'

describe Fairy do
  before :all do
    yml_path = File.expand_path(File.dirname(__FILE__) + "/../tools/cap_recipe/cluster.yml")
    @cluster = YAML.load_file(yml_path)
    @source = File.readlines("testdata.txt")
    @fairy = Fairy::Fairy.new
  end

  # input (local) + split
  it 'should split data & distribute them' do
    answer = @source.sort

    @fairy.input("testdata.txt").split(10).output("/tmp/fairy_spec_testdata.vf")
    system %{ fairy cat /tmp/fairy_spec_testdata.vf > /tmp/fairy_spec_testdata.txt }
    result = File.readlines("/tmp/fairy_spec_testdata.txt").sort

    result.should == answer
  end

  # exec
  it 'should print all node-names' do
    answer = @cluster["nodes"].sort
    
    result = []
    @fairy.exec(@cluster["nodes"].map{|n| "file://#{n}"}).map(%q{|uri|
      `hostname`.chomp
    }).here.each{|n|
      result << n
    }

    result.sort!

    result.should == answer
  end
end


