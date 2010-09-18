# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

require "fairy/share/block-source"

module Fairy
    module Interface
      def barrier(opts = nil)
	if opts[:cond].kind_of?(String)
	  opts[:cond] = BlockSource.new(opts[:cond])
	end
	barrier = Barrier.new(@fairy, opts)
	barrier.input = self
	barrier
      end
    end
    Fairy::def_filter_interface Interface


  class Barrier<IOFilter
    def backend_class_name
      "CBarrier"
    end
  end
end
