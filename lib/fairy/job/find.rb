# encoding: UTF-8

require "fairy/job/filter"

module Fairy
  class Find<Filter

    module Interface
      # filter.find(%{...})
      def find(block_source, opts = nil)
	block_source = BlockSource.new(block_source) 
	find = Find.new(@fairy, opts, block_source)
	find.input = self
	find
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BFind"
    end

    def value
      backend.value
    end
  end
end
