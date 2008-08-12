
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BEachElementSelector<BFilter
    include BInputtable

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source
    end

    def node_class_name
      "NEachElementSelector"
    end

    def create_node(processor)
      processor.create_njob(node_class_name, self, @opts, @block_source)
    end
  end
end
