# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/filter"

module Fairy
  class DirectProduct<Filter

    module Interface
      # jpb.direct_product(opts,...,filter,...,block_source, opts,...)
      def direct_product(*others)
	block_source = nil
	if others.last.kind_of?(String)
	  block_source = others.pop
	end
	others, opts = others.partition{|e| e.kind_of?(Job)}
	if opts.last.kind_of?(Hash)
	  h = opts.pop
	else
	  h = {}
	end
	opts.each{|e| h[e] = true}

	block_source = BlockSource.new(block_source) 
	dp = DirectProduct.new(@fairy, h, others, block_source)
	dp.input = self
	dp
      end
      alias product direct_product

      def *(other)
	direct_product(other, %{|e| e})
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
      "CDirectProduct"
    end
  end
end

