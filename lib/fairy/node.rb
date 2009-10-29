# encoding: UTF-8

#require "monitor"

require "deep-connect/deep-connect"

require "fairy/version"
require "fairy/share/conf"

#DeepConnect::Organizer.immutable_classes.push Array

module Fairy
  class Node

    def initialize
      @addr = nil
      @logger = nil

      @processor_seq = -1
      @processor_seq_mutex = Mutex.new

      @processors = []
      @processors_mutex = Mutex.new
      @processors_cv = ConditionVariable.new
    end

    attr_accessor :addr
    attr_reader :logger

    attr_reader :processors
    
#     def processors_dup
#       @processors.synchronize do
# 	@processors.dup
#       end
#     end

    def start(master_host, master_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Node", self)

      require "fairy/share/inspector"
      @deepconnect.export("Inspector", Inspector.new(self))

      require "fairy/share/log"
      @master_deepspace = @deepconnect.open_deepspace(master_host, master_port)
      @master = @master_deepspace.import("Master")
      @logger = @master.logger
      Log.type = "[N]"
      Log.logger = @logger
      Log.info(self, "Node Service Start")
      Log::info(self, "\tfairy version: #{Version}")
      Log::info(self, "\tRuby version: #{RUBY_VERSION}") 

      @master.register_node(self)
    end

    def processor_next_id
      @processor_seq_mutex.synchronize do
	@processor_seq += 1
      end
    end

    def create_processor
      proc = nil
      @processors_mutex.synchronize do
	processor_id = processor_next_id
#	Process.spawn("test/testn.rb", 
#		      "--controller", @deepconnect.local_id, 
#		      "--id", processor_id.to_s)
# 	pid = Process.fork{
# 	  Process.fork{
# 	    exec(CONF.RUBY_BIN, CONF.PROCESSOR_BIN,
# 		 "--node", @deepconnect.local_id.to_s, 
# 		 "--id", processor_id.to_s)
# 	  }
# 	}
# 	Process.wait pid

	pid = Process.fork {
	  NodeAPP.start_subcommand(CONF.RUBY_BIN, 
				   CONF.PROCESSOR_BIN,
				   "--node", @deepconnect.local_id.to_s, 
				   "--id", processor_id.to_s)
	}
 	Process.wait pid
	while !@processors[processor_id]
	  @processors_cv.wait(@processors_mutex)
	end
	@master.set_no_of_processors(self, @processors.size)
	@processors[processor_id]
      end
    end

    def terminate_processor(processor)
      deregister_processor(processor)
      begin
	processor.terminate
      rescue
	Log::debug(self, "Exception Rised in termination processor.")
	Log::debug_exception(self)
      end
# forkの仕組みが変わった.
#      Process.wait
    end

    def register_processor(processor)
#      @processors.synchronize do
      @processors_mutex.synchronize do
	@processors[processor.id] = processor
	processor.addr = @addr

	@processors_cv.broadcast
     end
    end

    def deregister_processor(processor)
#      @processors.synchronize do
      @processors_mutex.synchronize do
	@processors.delete(processor.id)
	@master.set_no_of_processors(self, @processors.size)

	@processors_cv.broadcast
      end
    end

      
    def Node.start(master_host, master_port)
      node = Node.new
      node.start(master_host, master_port)
    end
  end
end
