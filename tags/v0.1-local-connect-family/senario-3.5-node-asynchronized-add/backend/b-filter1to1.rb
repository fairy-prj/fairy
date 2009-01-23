require "backend/b-filter"
require "node/n-filter1to1"

module Fairy
  class BFilter1to1<BFilter

    def input=(input)
      super
      create_nodes
    end

    def create_nodes
      Thread.start do
	no = 0
	@input.each_node do |input_node|
	  node = create_node
	  node.input= input_node
	  input_node.output = node
	  add_node node
	  no += 1
	end
	self.number_of_nodes = no
      end
    end

#     def create_node
#       p self
#       graise "create_nodeが定義されていません"
#     end
  end
end
