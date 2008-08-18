
require "job/filter"

module Fairy
  class GroupBy<Filter

    module Interface
      def group_by(hash_block, opts = nil)
	group_by = GroupBy.new(@fairy, hash_block)
	group_by.input = self
	group_by
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, block_source, opts=nil)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "BGroupBy"
    end
  end
end
