# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy

  class Select<IOFilter
    module Interface
      def select(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	block_source = BlockSource.new(block_source) 
	mapper = Select.new(@fairy, opts, block_source)
	mapper.input=self
	mapper
      end

      alias find_all select

      def grep(regexp, opts = nil)
	select(%{|e| /#{regexp.source}/ === e}, opts)
      end
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "CSelect"
    end
  end

end
