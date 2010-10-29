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

  # select
  it 'should select multiples of 10' do
    answer = @source.map{|ln|
      n = ln.chomp.to_i
      n
    }.select{|n|
      (n % 10).zero?
    }.map{|n|
      "#{n}\n"
    }

    result = []
    @fairy.input("testdata.vf").map(%{|ln|
      n = ln.chomp.to_i
      n
    }).select(%{|n|
      (n % 10).zero?
    }).map(%q{|n|
      "#{n}"
    }).output("/tmp/fairy_spec_testdata.txt")
    
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer
  end

  # grep
  it 'should select 555' do
    answer = @source.map{|ln|
      n = ln.chomp
      n
    }.grep("555").map{|n|
      "#{n}\n"
    }

    @fairy.input("testdata.vf").map(%{|ln|
      n = ln.chomp
      n
    }).grep(/^555$/).output("/tmp/fairy_spec_testdata.txt")
    
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer
  end
end


