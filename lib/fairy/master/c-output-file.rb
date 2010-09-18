# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-output"

module Fairy
  class COutputFile<COutput
    Controller.def_export self

    def initialize(controller, opts)
      super
      @vfile = nil

#      @one_file_by_procs = @opts[:one_file_by_process]
    end

    def output(vf)
      @vfile = vf
    end

    def node_class_name
      "POutputFile"
    end

    def njob_creation_params
      [@vfile]
    end

# processorで１ファイルにまとめる機能は廃止

#     def create_nodes
#       return create_node_one_file if @one_file_by_procs
#       no = 0
#       @input.each_export do |input_export, input_njob|
# 	njob = create_and_add_node(input_export, input_njob)
# 	njob.add_input(nil)
# 	no += 1
#       end
#       self.number_of_nodes = no
#     end

#     def create_node_one_file
#       no = 0
#       input_processors = {}
#       @input.each_export do |input_export, input_njob|
# 	if njob = input_processors[input_njob.processor]
# 	  njob.add_input(input_export)
# 	else
# 	  njob = create_and_add_node(input_export, input_njob)
# 	  input_processors[njob.processor] = njob
# 	  no += 1
# 	end
#       end
#       for p, njob in input_processors
# 	njob.add_input(nil)
#       end
#       self.number_of_nodes = no
#     end


    def wait_all_output_finished
      @nodes_status_mutex.synchronize do
	while !all_node_outputted?
	  @nodes_status_cv.wait(@nodes_status_mutex)
	end
      end
    end

    def all_node_outputted?

      return false unless @number_of_nodes

      all_outputted = true
      each_node(:exist_only) do |node|
	st = @nodes_status[node]
	all_outputted &&= [:ST_FINISH, :ST_OUTPUT_FINISH].include?(st)
      end
      all_outputted
    end

  end
end
