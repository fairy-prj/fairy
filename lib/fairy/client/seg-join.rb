# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy
  class SegJoin<IOFilter

    module Interface
      # jpb.seg_join(opts,...,filter,...,block_source, opts,...)
      def seg_join(*others)
	block_source = nil
	if others.last.kind_of?(String)
	  block_source = others.pop
	elsif others.last.kind_of?(Hash) and others[-2].kind_of?(String)
	  block_source = others.delete_at(-2)
	end
	others, opts = others.partition{|e| e.kind_of?(Filter)}
	if opts.last.kind_of?(Hash)
	  h = opts.pop
	else
	  h = {}
	end
	opts.each{|e| h[e] = true}

	pres = others.collect{|o|
	  p = PreJoinedFilter.new(@fairy, h)
	  p.input = o
	  p
	}

	block_source = BlockSource.new(block_source) 
	join = SegJoin.new(@fairy, h, pres, block_source)
	join.input = self
	join
      end
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, others, block_source)
      super(fairy, opts, others.collect{|o| o.backend}, block_source)
      @others = others
      @block_source
      @opts = opts
    end

    def backend_class_name
      "CSegJoin"
    end

    class PreJoinedFilter<IOFilter
      def backend_class_name
	"CSegJoin::CPreSegJoinFilter"
      end
    end

  end
end

