
module Fairy
  module BInputtable

    def input=(input)
      super
      start_create_nodes
    end

    def start_create_nodes
      Thread.start do
	create_nodes
      end
    end

    def create_nodes
      no = 0
      @input.each_export do |export|
	create_and_add_node(export)
	no += 1
      end
      self.number_of_nodes = no
    end

    def create_and_add_node(export)
      node = create_node
      node.input= export
      export.output = node.import
      add_node node
      node
    end

  end
end
