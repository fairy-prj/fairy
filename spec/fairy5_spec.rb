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

  # mapf + group_by + map (word count)
  it 'should count unique values' do
    answer = {}
    @source_multi.each{|ln|
      nums = ln.split.map{|s| s.to_i}
      nums.each{|n|
        answer[n] ||= 0
        answer[n] += 1
      }
    }

    @fairy.input("testdata_multi.vf").mapf(%{|ln|
      nums = ln.split.map{|s| s.to_i}
      nums
    }).group_by(%{|n| n.to_s}).map(%q{|bag|
      "#{bag.key}\t#{bag.size}"
    }).output("/tmp/fairy_spec_testdata_multi.txt")
      
    result = File.readlines("/tmp/fairy_spec_testdata_multi.txt").inject({}){|res,ln|
      ent = ln.split.map{|s| s.to_i}
      res[ent[0]] = ent[1] 
      res
    }

    result.should == answer
  end
end


