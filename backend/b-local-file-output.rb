# encoding: UTF-8

require "backend/boutput"

module Fairy
  class BLFileOutput<BOutput
    Controller.def_export(self)

    def initialize(controller, opts=nil)
      @imports = Queue.new
      super
    end

    def output(job)
      @job = job
    end

    def node_class_name
      "NLFileOutput"
    end

    def create_nodes
      no = 0
      input_processors = {}
      @input.each_export do |input_export, input_njob|
	if njob = input_processors[input_njob.processor]
	  njob.add_input(input_export)
	else
	  njob = create_and_add_node(input_export, input_njob)
	  input_processors[njob.processor] = njob
	  no += 1

	  import = Import.new
	  @imports.push import
	  njob.export.output = import
	  import.no_import = 1
	end
      end
      for p, njob in input_processors
	njob.add_input(nil)
      end
      self.number_of_nodes = no

      @imports.push nil
    end

    def each(&block)
      while import = @imports.pop
	import.each do |e|
	  block.call e
	end
      end
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
	all_outputted &= [:ST_FINISH, :ST_OUTPUT_FINISH].include?(st)
      end
      all_outputted
    end

  end
end
