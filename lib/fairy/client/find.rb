# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy
  class Find<IOFilter

    module Interface
      # filter.find(%{...})
      def find(block_source, opts = nil)
	block_source = BlockSource.new(block_source) 
	find = Find.new(@fairy, opts, block_source)
	find.input = self
	find
      end
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "CFind"
    end

    def value
      backend.value
    end
  end
end
