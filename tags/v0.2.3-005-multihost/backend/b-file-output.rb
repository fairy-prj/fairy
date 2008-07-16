
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
