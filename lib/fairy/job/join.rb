# encoding: UTF-8

require "fairy/job/filter"

module Fairy
  class Join<Filter

    module Interface
      # jpb.join(opts,...,filter,...,block_source, opts,...)
      def join(*others)
	block_source = nil
	if others.last.kind_of?(String)
	  block_source = others.pop
	elsif others.last.kind_of?(Hash) and others[-2].kind_of?(String)
	  block_source = others.delete_at(-2)
	end
	others, opts = others.partition{|e| e.kind_of?(Job)}
	if opts.last.kind_of?(Hash)
	  h = opts.pop
	else
	  h = {}
	end
	opts.each{|e| h[e] = true}

	block_source = BlockSource.new(block_source) 
	join = Join.new(@fairy, h, others, block_source)
	join.input = self
	join
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, others, block_source)
      super(fairy, opts, others.collect{|o| o.backend}, block_source)
      @others = others
      @block_source
      @opts = opts
    end

    def backend_class_name
      "BJoin"
    end
  end
end

