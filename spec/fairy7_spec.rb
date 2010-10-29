# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'rspec'

require 'fairy'

describe Fairy do
  before :all do
    @source_join_a = File.readlines("testdata_join_a.txt")
    @source_join_b = File.readlines("testdata_join_b.txt")
    @fairy = Fairy::Fairy.new
  end

  # equijoin
  it 'should do inner join' do
    a = File.readlines("testdata_join_a.txt").map{|ln| rec = ln.split; rec}
    b = File.readlines("testdata_join_b.txt").map{|ln| rec = ln.split; rec}

    index = {}
    b.each{|id,val|
      index[id] ||= []
      index[id] << val
    }

    answer = []
    a.each{|id,val|
      next unless index[id]
      index[id].each{|val_b|
        answer << [id, val, val_b]
      }
    }

    answer = answer.sort_by{|ent| "%05d-%s-%s" % ent}.map{|ent|
      ent.join("\t") + "\n"
    }

    a = @fairy.input("testdata_join_a.vf").map(%{|ln| rec = ln.split; rec})
    b = @fairy.input("testdata_join_b.vf").map(%{|ln| rec = ln.split; rec})

    joined = a.equijoin(b, 0)

    joined.map(%{|from_a, from_b|
      [from_a[0], from_a[1], from_b[1]]
    }).sort_by(%{|ent| "%05d-%s-%s" % ent}).map(%{|ent|
      ent.join("\t")
    }).output("/tmp/fairy_spec_testdata_join.txt")

    result = File.readlines("/tmp/fairy_spec_testdata_join.txt")

    result.should == answer
  end
end


