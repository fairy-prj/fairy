
require "job/filter"

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

	zip = Zipper.new(@fairy, others, block_source, h)
	zip.input = self
	zip
      end
    end
    Fairy::def_job_interface Interface

    ZIP_BY_SUBSTREAM = :ZIP_BY_SUBSTREAM

    def initialize(fairy, others, block_source, opts=nil)
      super(fairy, others.collect{|o| o.backend}, block_source, opts)
      @others = others
      @block_source
      @opts = opts
    end

    def backend_class_name
      "BZipper"
    end
  end
end

