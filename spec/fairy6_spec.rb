# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'rspec'

require 'fairy'

describe Fairy do
  before :all do
    @source = File.readlines("testdata.txt")
    @fairy = Fairy::Fairy.new
  end

  # sort_by
  it 'should sort data' do
    answer = @source.map{|ln|
      n = ln.chomp.to_i
      n
    }.sort{|a,b|
      a <=> b
    }.map{|n|
      "#{n}\n"
    }

    @fairy.input("testdata.vf").map(%{|ln|
      n = ln.chomp.to_i
      n
    }).sort_by(%{|n| n.to_i}).map(%q{|n|
      "#{n}"
    }).output("/tmp/fairy_spec_testdata.txt")
                                  
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer

    @fairy.input("testdata.vf").map(%{|ln|
      n = ln.chomp.to_i
      n
    }).sort_by(%{|n| -(n.to_i)}).map(%q{|n|
      "#{n}"
    }).output("/tmp/fairy_spec_testdata.txt")
    
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer.reverse
  end
end


