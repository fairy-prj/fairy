# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"
require "fairy/share/block-source"

module Fairy

  class Map<IOFilter
    module Interface
      def map(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	block_source = BlockSource.new(block_source) 
	mapper = Map.new(@fairy, opts, block_source)
	mapper.input = self
	mapper
      end
      alias collect map
     
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "CMap"
    end
  end
end
