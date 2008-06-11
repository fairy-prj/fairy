
require "backend/b-filter1to1"
require "node/n-each-substream-mapper"

module Fairy
  class BEachSubStreamMapper<BFilter1to1
    def initialize(controller, block_source)
      super(controller)
      @block_source = block_source
    end

    def node_class
      NEachSubStreamMapper
    end
  end
end
