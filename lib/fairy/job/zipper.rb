# encoding: UTF-8

require "fairy/job/filter"

module Fairy
  class Zipper<Filter

    module Interface
      # jpb.zip(opts,...,filter,...,block_source, opts,...)
      def zip(*others)
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
	zip = Zipper.new(@fairy, h, others, block_source)
	zip.input = self
	zip
      end
    end
    Fairy::def_job_interface Interface

    ZIP_BY_SUBSTREAM = :ZIP_BY_SUBSTREAM

    def initialize(fairy, opts, others, block_source)
      super(fairy, opts, others.collect{|o| o.backend}, block_source)
      @others = others
      @block_source
      @opts = opts
    end

    def backend_class_name
      "BZipper"
    end
  end
end

