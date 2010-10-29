# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'rspec'

require 'fairy'

describe Fairy do
  before :all do
    @source_multi = File.readlines("testdata_multi.txt")
    @fairy = Fairy::Fairy.new
  end

  # input/here
  it 'should open a vfile & print the contents' do
    contents = []
    @fairy.input("testdata_multi.vf").here.each{|ln|
      contents << ln
    }
    contents.should == @source_multi
  end

  # output (local)
  it 'should output to a local file' do
    @fairy.input("testdata_multi.vf").output("/tmp/fairy_spec_testdata_multi.txt")
    contents = File.readlines("/tmp/fairy_spec_testdata_multi.txt")
    contents.should == @source_multi
  end

  # output (vfile)
  it 'should output to remote files (a vfile)' do
    @fairy.input("testdata_multi.vf").output("/tmp/fairy_spec_testdata_multi.vf")
    system %{ fairy cp /tmp/fairy_spec_testdata_multi.vf /tmp/fairy_spec_testdata_multi.txt }
    contents = File.readlines("/tmp/fairy_spec_testdata_multi.txt")
    contents.should == @source_multi
  end
end


