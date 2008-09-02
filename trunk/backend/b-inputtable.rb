

module Fairy
  module BInputtable

    def initialize(*rests)
      super
      @input = nil
    end

    def input=(input)
      @input = input
#      input.output = @input

      start_create_nodes
    end

    attr_reader :input

    def start_create_nodes
      puts "START_CREATE_NODES: #{self}"
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
puts "CREATE_NODES: #{self}.number_of_nodes=#{no}"
      self.number_of_nodes = no
    end

    def create_and_add_node(input_export, input_node)
      node = nil
      @controller.assign_inputtable_processor(self, 
					      @input, 
					      input_node, 
					      input_export) do |processor|
	node = create_node(processor)
      end
      node.input= input_export
      input_export.output = node.import
      node
    end

  end
end
