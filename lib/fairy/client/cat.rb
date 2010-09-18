# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy
  class Cat<IOFilter

    module Interface
      # jpb.cat(opts,...,filter,...,opts,...)
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
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, others)
      super(fairy, opts, others.collect{|o| o.backend})
      @others = others
      @block_source
      @opts = opts
    end

    def backend_class_name
      "CCat"
    end
  end
end

