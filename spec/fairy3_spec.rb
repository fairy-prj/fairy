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
    @source_multi = File.readlines("testdata_multi.txt")
    @fairy = Fairy::Fairy.new
  end

  # map (=collect)
  it 'should strip LFs & convert values to integer & add one' do
    answer = @source.map{|ln|
      n = ln.chomp.to_i + 1
      "#{n}\n"
    }

    @fairy.input("testdata.vf").map(%q{|ln|
      n = ln.chomp.to_i + 1
      "#{n}"
    }).output("/tmp/fairy_spec_testdata.txt")
    
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer

    @fairy.input("testdata.vf").collect(%q{|ln|
      n = ln.chomp.to_i + 1
      "#{n}"
    }).output("/tmp/fairy_spec_testdata.txt")
    
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer
  end

  # map + map
  it 'should add one + double values' do
    answer = @source.map{|ln|
      n = ln.chomp.to_i + 1
      n *= 2
      "#{n}\n"
    }

    @fairy.input("testdata.vf").map(%{|ln|
      n = ln.chomp.to_i + 1
      n
    }).map(%q{|n|
      n *= 2
      "#{n}"
    }).output("/tmp/fairy_spec_testdata.txt")
    
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer
  end

  # map + map + map
  it 'should add one + double values + subtract three' do
    answer = @source.map{|ln|
      n = ln.chomp.to_i + 1
      n *= 2
      n -= 3
      "#{n}\n"
    }

    @fairy.input("testdata.vf").map(%{|ln|
      n = ln.chomp.to_i + 1
      n
    }).map(%{|n|
      n *= 2
      n
    }).map(%q{|n|
      n -= 3
      "#{n}"
    }).output("/tmp/fairy_spec_testdata.txt")
    
    result = File.readlines("/tmp/fairy_spec_testdata.txt")

    result.should == answer
  end

  # map_flatten (=mapf)
  it 'should split lines and flatten values' do
    answer = @source_multi.map{|ln|
      nums = ln.split.map{|s| s.to_i}
      nums
    }.flatten.map{|n|
      n + 1
      "#{n}\n"
    }

    @fairy.input("testdata_multi.vf").map_flatten(%{|ln|
      nums = ln.split.map{|s| s.to_i}
      nums
    }).map(%q{|n|
      n + 1
      "#{n}"
    }).output("/tmp/fairy_spec_testdata_multi.txt")

    result = File.readlines("/tmp/fairy_spec_testdata_multi.txt")

    result.should == answer

    @fairy.input("testdata_multi.vf").mapf(%{|ln|
      nums = ln.split.map{|s| s.to_i}
      nums
    }).map(%q{|n|
      n + 1
      "#{n}"
    }).output("/tmp/fairy_spec_testdata_multi.txt")

    result = File.readlines("/tmp/fairy_spec_testdata_multi.txt")

    result.should == answer
  end
end


