# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy
  class BasicGroupBy<Filter

    module Interface
      def basic_group_by(hash_block, opts = nil)
	hash_block = BlockSource.new(hash_block) 
	group_by = BasicGroupBy.new(@fairy, opts, hash_block)
	group_by.input = self
	group_by
      end
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "CBasicGroupBy"
    end
  end

  class BasicMGroupBy<IOFilter

    module Interface
      def basic_mgroup_by(hash_block, opts = nil)
	hash_block = BlockSource.new(hash_block) 
	mgroup_by = MBasicGroupBy.new(@fairy, opts, hash_block)
	mgroup_by.input = self
	mgroup_by
      end
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BBasicMGroupBy"
    end
  end
end
