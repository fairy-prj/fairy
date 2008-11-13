
require "share/exceptions"

module Fairy
  module BInputtable

    def initialize(*rests)
      super
      @input = nil

      @create_node_thread = nil
      @create_node_mutex = Mutex.new
    end

    def input=(input)
      @input = input
#      input.output = @input

      start_create_nodes
    end

    attr_reader :input

    def start_create_nodes
      puts "START_CREATE_NODES: #{self}"
      @create_node_thread = Thread.start{
	create_nodes
      }
      nil
    end

    def create_nodes
      begin
	no = 0
	@input.each_export do |export, node|
	  @create_node_mutex.synchronize do
	    create_and_add_node(export, node)
	    no += 1
	  end
	end
      rescue BreakCreateNode
	# do nothing
	puts "BREAK CREATE NODE: ${self}" 
      ensure
	puts "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	self.number_of_nodes = no
      end
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

    def break_create_node
      @create_node_mutex.synchronize do
	@create_node_thread.raise BreakCreateNode
      end
    end
  end
end
