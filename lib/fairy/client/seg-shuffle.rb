# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy
  class SegShuffle<IOFilter
    module Interface
      def seg_shuffle(block_source, opts = nil)
	block_source = BlockSource.new(block_source) 
	shuffle = SegShuffle.new(@fairy, opts, block_source)
	shuffle.input = self
	shuffle
      end
      alias sshuffle shuffle

      def seg_eshuffle(block_source, opts = nil)
	map_source = %{|i, o| proc{#{block_source}}.call(i).each{|e| o.push e}}
	seg_shuffle(map_source, opts)
      end
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "CSegShuffle"
    end
  end
end
