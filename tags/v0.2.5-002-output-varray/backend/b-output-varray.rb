
require "controller"
require "backend/boutput"

require "share/varray"

module Fairy
  class BOutputVArray<BOutput
    Controller.def_export self

    def initialize(controller, opts)
      super
      @varray = VArray.new

      @node_id = 0
    end

    attr_reader :varray

    def node_class_name
      "NOutputVArray"
    end

    def njob_creation_params
      @node_id += 1
      [@node_id-1]
#      []
    end

    def number_of_nodes=(no_nodes)
      super
      ary = Array.new(no_nodes)
      @varray.set_arrays(ary)
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
