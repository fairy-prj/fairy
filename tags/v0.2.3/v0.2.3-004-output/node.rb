
require "deep-connect/deep-connect.rb"

module Fairy
  class Node

    PROCESSOR_BIN = "bin/processor"

    def initialize
      @addr = nil

      @processors = []
      @processors_mutex = Mutex.new
      @processors_cv = ConditionVariable.new
    end

    attr_accessor :addr

    def start(master_host, master_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Node", self)

      @master_deepspace = @deepconnect.open_deepspace(master_host, master_port)
      @master = @master_deepspace.import("Master")
      @master.register_node(self)
    end

    def create_processor
      @processors_mutex.synchronize do
	processor_id = @processors.size
#	Process.spawn("test/testn.rb", 
#		      "--controller", @deepconnect.local_id, 
#		      "--id", processor_id.to_s)
	Process.fork do
	  exec(PROCESSOR_BIN,
	       "--node", @deepconnect.local_id.to_s, 
	       "--id", processor_id.to_s)
	end
	while !@processors[processor_id]
	  @processors_cv.wait(@processors_mutex)
	end
	@processors[processor_id]
      end
    end
      
    def register_processor(processor)
      @processors_mutex.synchronize do
	@processors[processor.id] = processor
	processor.addr = @addr
	@processors_cv.broadcast
      end
    end

    def Node.start(master_host, master_port)
      node = Node.new
      node.start(master_host, master_port)
    end
  end
end
