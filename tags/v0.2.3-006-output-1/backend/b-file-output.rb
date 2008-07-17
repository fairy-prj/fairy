
require "backend/boutput"

module Fairy
  class BFileOutput<BOutput

    def initialize(controller)
      super(controller)
      @vfile = nil
    end

    def output(vf)
      @vfile = vf
    end

    def node_class_name
      "NFileOutput"
    end

    def create_nodes
      no = 0
      input_processors = {}
puts "create_nodes: BEGIN"
      @input.each_export do |input_export, input_njob|
puts "create_nodes: *#{no}"
	if njob = input_processors[input_njob.processor]
puts "create_nodes: *A"
	  njob.add_input(input_export)
puts "create_nodes: *A1"
	else
puts "create_nodes: *B"
	  njob = create_and_add_node(input_export, input_njob)
	  input_processors[njob.processor] = njob
	  no += 1
puts "create_nodes: *B1"
	end
puts "create_nodes: *E"
      end
puts "create_nodes: END"
      for p, njob in input_processors
	njob.add_input(nil)
      end
      self.number_of_nodes = no
    end

    def create_node(processor)
      processor.create_njob(node_class_name, self, @vfile)
    end

    def wait_all_output_finished
      while !all_node_outputted?
	@nodes_status_mutex.synchronize do
	  @nodes_status_cv.wait(@nodes_status_mutex)
	end
      end
    end

    def all_node_outputted?
      return false unless @number_of_nodes

      all_outputted = true
      each_node(:exist_only) do |node|
	st = @nodes_status[node]
	all_imported &= [:ST_FINISH, :ST_OUTPUT_FINISH].include?(st)
      end
      all_outputted
    end

  end
end
