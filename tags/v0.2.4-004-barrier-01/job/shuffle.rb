
require "job/filter"

module Fairy
  class Shuffle<Filter
    def initialize(fairy, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BShuffle"
    end
  end
end
