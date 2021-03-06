# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-output"

module Fairy
  class COutputNull<COutput
    Controller.def_export self

    def initialize(controller, opts)
      super
    end

    def node_class_name
      "POutputNull"
    end

#    def njob_creation_params
#    end

    def number_of_nodes=(no_nodes)
      super
    end

    def wait_all_output_finished
      @nodes_status_mutex.synchronize do
	while !all_node_outputted?
	  @nodes_status_cv.wait(@nodes_status_mutex)
	end
      end
    end

    def all_node_outputted?
      return false unless @nodes_mutex.synchronize{@number_of_nodes}

      each_node(:exist_only) do |node|
	st = @nodes_status[node]
	return false unless [:ST_FINISH, :ST_OUTPUT_FINISH].include?(st)
      end
      true
    end
  end
end
