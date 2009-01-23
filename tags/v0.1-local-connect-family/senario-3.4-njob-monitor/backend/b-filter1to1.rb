require "backend/b-filter"
require "node/n-filter1to1"

module Fairy
  class BFilter1to1<BFilter

    def input=(input)
      super

      self.nodes = input.nodes.collect{|input_node|
	node = create_node
	node.input= input_node
	input_node.output = node
	node
      }
    end

    def create_node
      raise "create_nodeが定義されていません"
    end
  end
end
