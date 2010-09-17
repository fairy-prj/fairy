# encoding: UTF-8

require "fairy/job/group-by"

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
    Fairy::def_job_interface Interface

    class PreSort<Filter
      def initialize(fairy, opts, block_source)
	super
	@block_source = block_source
      end

      def backend_class_name
	"BSort::BPreSort"
      end
    end

    class PostSort<Filter
      def initialize(fairy, opts, block_source)
	super
	@block_source = block_source
      end

      def backend_class_name
	"BSort::BPostSort"
      end
    end

  end
end

