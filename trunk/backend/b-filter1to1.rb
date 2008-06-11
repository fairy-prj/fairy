require "backend/b-filter"
require "node/n-filter1to1"

module Fairy
  class BFilter1to1<BFilter

    def input=(input)
      super

      self.nodes = input.nodes.collect{|input_node|
	node = node_class.new(@block_source)
	node.input= input_node
	input_node.output = node
	node
      }
    end
  end
end
