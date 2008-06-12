
require "backend/b-filter1to1"
require "node/n-each-element-mapper"

module Fairy
  class BEachElementMapper<BFilter1to1
    def initialize(controller, block_source)
      super(controller)
      @block_source = block_source
    end

    def node_class
      NEachElementMapper
    end

    def create_node
      node_class.new(self, @block_source)
    end
  end
end
