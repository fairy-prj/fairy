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
	@input.each_export do |export|
	  node = create_node
	  node.input= export
	  export.output = node.import
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
