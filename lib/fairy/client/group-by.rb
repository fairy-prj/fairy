# encoding: UTF-8

require "fairy/job/filter"

module Fairy
  class GroupBy<Filter

    module Interface
      def group_by(hash_block, opts = nil)
	hash_block = BlockSource.new(hash_block) 
	group_by = GroupBy.new(@fairy, opts, hash_block)
	group_by.input = self
	group_by
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BGroupBy"
    end
  end

  class MGroupBy<Filter

    module Interface
      def mgroup_by(hash_block, opts = nil)
	hash_block = BlockSource.new(hash_block) 
	mgroup_by = MGroupBy.new(@fairy, opts, hash_block)
	mgroup_by.input = self
	mgroup_by
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BMGroupBy"
    end
  end
end
