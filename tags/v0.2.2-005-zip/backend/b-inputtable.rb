

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
      puts "START CREATE_NODE: 0#{self}"
      no = 0
      @input.each_export do |export, node|
      puts "START CREATE_NODE: 1#{self}"
	create_and_add_node(export, node)
	no += 1
      puts "START CREATE_NODE: 2#{self}"
      end
      puts "START CREATE_NODE: 3#{self}"
      self.number_of_nodes = no
      puts "END CREATE_NODE: 4#{self}"
    end

    def create_and_add_node(export, node)
      processor = @controller.assign_inputtable_processor(@input, node, export)
      node = create_node(processor)
      node.input= export
      export.output = node.import
      add_node node
      node
    end

  end
end
