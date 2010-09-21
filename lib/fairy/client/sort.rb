# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/basic-group-by"

module Fairy

  module Sort

    module Interface
      def sort_by(cmp_proc, opts=nil)
	cmp_proc = BlockSource.new(cmp_proc)
	pre_sort = Sort::PreSort.new(@fairy, opts, cmp_proc)
	pre_sort.input = self
	post_sort = Sort::PostSort.new(@fairy, opts, cmp_proc)
	post_sort.input = pre_sort
	post_sort
      end
    end
    Fairy::def_filter_interface Interface

    class PreSort<IOFilter
      def initialize(fairy, opts, block_source)
	super
	@block_source = block_source
      end

      def backend_class_name
	"CSort::CPreSort"
      end
    end

    class PostSort<IOFilter
      def initialize(fairy, opts, block_source)
	super
	@block_source = block_source
      end

      def backend_class_name
	"CSort::CPostSort"
      end
    end

  end
end

