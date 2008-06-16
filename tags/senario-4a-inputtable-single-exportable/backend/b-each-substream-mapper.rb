
require "backend/b-filter"
require "backend/b-inputtable"

require "node/n-each-substream-mapper"

module Fairy
  class BEachSubStreamMapper<BFilter
    include BInputtable

    def initialize(controller, block_source)
      super(controller)
      @block_source = block_source
    end

    def node_class
      NEachSubStreamMapper
    end

    def create_node
      node_class.new(self, @block_source)
    end
  end
end
