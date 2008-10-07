require "uri"

require "backend/binput"
require "share/vfile"

module Fairy
  class BLFileInput<BInput
    Controller.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL)")

    def BLFileInput.open(controller, opts = nil)
      blfileinput = BFile.new(controller, opts)
      blfileinput.open(descripter)
      blfileinput
    end

    def initialize(controller, opts = nil)
      super
    end

    def node_class_name
      "NLFileInput"
    end

    def start(job)
      @job = job
      super()
    end

    def create_and_start_nodes
      if @opts[:split_size]
	create_and_start_nodes_split
      else
	create_and_start_nodes1
      end
    end

    def create_and_start_nodes1
      begin
	no = 0
	@create_node_mutex.synchronize do
	  nlfileinput = nil
	  @controller.assign_new_processor(self) do |processor|
	    nlfileinput = create_node(processor)
	  end
	  no = 1
	  Thread.start do
	    @job.open do |io|
	      nlfileinput.open(io)
	      wait_input_finished(nlfileinput)
	    end
	  end
	end
      rescue BreakCreateNode
	# do nothing
	puts "BREAK CREATE NODE: #{self}" 
      ensure
	self.number_of_nodes = no
      end
      nil
    end

    def create_and_start_nodes_split
      begin
	no_nodes = 0
	@job.split_opens(@opts[:split_size]) do |io|
	  @create_node_mutex.synchronize do
	    no_nodes += 1
	    nlfileinput = nil
	    @controller.assign_new_processor(self) do |processor|
	      nlfileinput = create_node(processor)
	    end
	    Thread.start(nlfileinput) do |nlfi|
	      begin
		nlfi.open(io)
		wait_input_finished(nlfi)
	      ensure
		io.close
	      end
	    end
	  end
	end
      rescue BreakCreateNode
	# do nothing
	puts "BREAK CREATE NODE: #{self}" 
      ensure
	self.number_of_nodes = no_nodes
      end
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
