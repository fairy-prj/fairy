
require "job/filter"

module Fairy
    module Interface
      def barrier(opts = nil)
	barrier = Barrier.new(@fairy, opts)
	barrier.input = self
	barrier
      end
    end
    Fairy::def_job_interface Interface


  class Barrier<Filter
    def initialize(fairy, opts)
      super
      @opts = opts
    end

    def backend_class_name
      "BBarrier"
    end
  end
end
