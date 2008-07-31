

module Fairy
  module BInputtable

    def input=(input)
      begin
	super
      rescue NoMethodError
      end
      start_create_nodes
    end

    def start_create_nodes
      Thread.start do
	create_nodes
      end
    end

    def create_nodes
      no = 0
      @input.each_export do |export, node|
	create_and_add_node(export, node)
	no += 1
      end
      self.number_of_nodes = no
    end

    def create_and_add_node(input_export, input_node)
      processor = @controller.assign_inputtable_processor(self, @input, input_node, input_export)
      node = create_node(processor)
      add_node node
      node.input= input_export
      input_export.output = node.import
      node
    end

  end
end
