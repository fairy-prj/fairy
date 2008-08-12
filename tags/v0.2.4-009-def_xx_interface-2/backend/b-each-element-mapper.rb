
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BEachElementMapper<BFilter
    include BInputtable

    def initialize(controller, block_source, opts=nil)
      super(controller, opts)
      @block_source = block_source
    end

    def node_class_name
      "NEachElementMapper"
    end

    def create_node(processor)
      processor.create_njob(node_class_name, self, @block_source, @opts)
    end
  end
end
