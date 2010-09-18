# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy
  class SegZip<IOFilter

    module Interface
      # jpb.seg_zip(opts,...,filter,...,block_source, opts,...)
      def seg_zip(*others)
	block_source = nil
	if others.last.kind_of?(String)
	  block_source = others.pop
	end
	others, opts = others.partition{|e| e.kind_of?(Filter)}
	if opts.last.kind_of?(Hash)
	  h = opts.pop
	else
	  h = {}
	end
	opts.each{|e| h[e] = true}

	pres = others.collect{|o|
	  p = PreSegZipFilter.new(@fairy, h)
	  p.input = o
	  p
	}

	block_source = BlockSource.new(block_source) 
	zip = SegZip.new(@fairy, h, pres, block_source)
	zip.input = self
	zip
      end
    end
    Fairy::def_filter_interface Interface

#    ZIP_BY_SEGMENT = :ZIP_BY_SEGMENT

    def initialize(fairy, opts, others, block_source)
      super(fairy, opts, others.collect{|o| o.backend}, block_source)
      @others = others
      @block_source
      @opts = opts
    end

    def backend_class_name
      "CSegZip"
    end

    class PreZippedFilter<Filter
      def backend_class_name
	"CSegZip::CPreSegZipFilter"
      end
    end
  end
end

