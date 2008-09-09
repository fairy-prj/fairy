
require "job/filter"

module Fairy
  class Shuffle<Filter
    module Interface
      def shuffle(block_source, opts = nil)
	shuffle = Shuffle.new(@fairy, opts, block_source)
	shuffle.input = self
	shuffle
      end
      alias sshuffle shuffle

      def eshuffle(block_source, opts = nil)
	map_source = %{|i, o| proc{#{block_source}}.call(i).each{|e| o.push e}}
	shuffle(map_source, opts)
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "BShuffle"
    end
  end
end
