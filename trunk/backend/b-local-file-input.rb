require "uri"

require "backend/binput"
require "share/vfile"

module Fairy
  class BLFileInput<BInput
#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL)")

    def BLFileInput.open(controller, opts = nil)
      blfileinput = BFile.new(controller, opts)
      blfileinput.open(descripter)
      blfileinput
    end

    def initialize(controller, opts = nil)
      super(controller, opts)
    end

    def node_class_name
      "NLFileInput"
    end

    def start(job)
      if @opts[:split_size]
	start_split(job)
      else
	start1(job)
      end
    end

    def start1(job)
      processor = @controller.assign_new_processor(self)
      nlfileinput = processor.create_njob(node_class_name, self)
      add_node nlfileinput
      self.number_of_nodes = 1
      Thread.start do
	job.open do |io|
	  nlfileinput.open(io)
	  wait_input_finished(job)
	end
      end
    end

    def start_split(job)
      no_nodes = 0
      job.split_opens(@opts[:split_size]) do |io|
	no_nodes += 1
	processor = @controller.assign_new_processor(self)
	nlfileinput = processor.create_njob(node_class_name, self)
	add_node nlfileinput
	Thread.start(nlfileinput) do |nlfi|
	  begin
	    nlfi.open(io)
	    wait_input_finished(nlfi)
	  ensure
	    io.close
	  end
	end
      end
      self.number_of_nodes = no_nodes
    end

    def wait_input_finished(njob)
      while !njob_input_finished?(njob)
	@nodes_status_mutex.synchronize do
	  @nodes_status_cv.wait(@nodes_status_mutex)
	end
      end
    end

    def njob_input_finished?(njob)
      return false
      st = @nodes_status[njob]
      [:ST_WAIT_EXPORT_FINISH, :ST_EXPORT_FINISH, :ST_FINISH, :ST_OUTPUT_FINISH].include?(st)
    end

  end
end
