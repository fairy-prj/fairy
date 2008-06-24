
require "node/n-each-element-mapper"

module Fairy
  class BEachElementMapper
    def initialize(controller, block_source)
      @controller = controller
      @block_source = block_source
    end

    def input=(input)
      @input = input

      for input_node in input.nodes
	node = NEachElementMapper.new(@block_source)
	node.input= input_node
      end
    end
  end
end
