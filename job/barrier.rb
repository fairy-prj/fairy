# encoding: UTF-8

require "job/filter"

require "share/block-source"

module Fairy
    module Interface
      def barrier(opts = nil)
	if opts[:cond].kind_of?(String)
	  opts[:cond] = BlockSource.new(opts[:cond])
	end
	barrier = Barrier.new(@fairy, opts)
	barrier.input = self
	barrier
      end
    end
    Fairy::def_job_interface Interface


  class Barrier<Filter
    def backend_class_name
      "BBarrier"
    end
  end
end
