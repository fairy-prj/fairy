
require "job/filter"

module Fairy
  class Shuffle<Filter
    module Interface
      def shuffle(block_source, opts = nil)
	shuffle = Shuffle.new(@fairy, block_source)
	shuffle.input = self
	shuffle
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, block_source, opts=nil)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "BShuffle"
    end
  end
end
