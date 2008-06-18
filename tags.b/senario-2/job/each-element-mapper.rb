
require "job/filter"

module Fairy
  class EachElementMapper<Filter
    def initialize(fairy, block_source)
      super
      @block_source = block_source
    end

    def backend_class
      BEachElementMapper
    end
  end

  class BEachElementMapper;end
end
