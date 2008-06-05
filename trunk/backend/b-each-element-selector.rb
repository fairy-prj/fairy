
require "backend/bjob"
require "node/n-each-element-selector"

module Fairy
  class BEachElementSelector<BJob
    def initialize(controller, block_source)
      super(controller)
      @block_source = block_source
    end

    def input=(input)
      @input = input

      self.nodes = input.nodes.collect{|input_node|
	node = NEachElementSelector.new(@block_source)
	node.input= input_node
	node
      }
    end
  end
end
