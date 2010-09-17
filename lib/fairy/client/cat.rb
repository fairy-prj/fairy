# encoding: UTF-8

require "fairy/job/filter"

module Fairy
  class Cat<Filter

    module Interface
      # jpb.zip(opts,...,filter,...,opts,...)
      def cat(*others)
	others, opts = others.partition{|e| e.kind_of?(Job)}
	if opts.last.kind_of?(Hash)
	  h = opts.pop
	else
	  h = {}
	end
	opts.each{|e| h[e] = true}

	cat = Cat.new(@fairy, h, others)
	cat.input = self
	cat
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, others)
      super(fairy, opts, others.collect{|o| o.backend})
      @others = others
      @block_source
      @opts = opts
    end

    def backend_class_name
      "BCat"
    end
  end
end

