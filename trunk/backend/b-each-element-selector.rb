
require "backend/b-filter1to1"
require "node/n-each-element-selector"

module Fairy
  class BEachElementSelector<BFilter1to1
    def initialize(controller, block_source)
      super(controller)
      @block_source = block_source
    end

    def node_class
      NEachElementSelector
    end

    def create_node
      node_class.new(@block_source)
    end
  end
end
