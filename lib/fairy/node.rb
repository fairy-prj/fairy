# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

#require "monitor"

require "timeout"

require "xthread"
require "fiber-mon"

require "deep-connect"

require "fairy/version"
require "fairy/share/conf"


#DeepConnect::Organizer.immutable_classes.push Array

module Fairy
  class Node

    def initialize
      @id = nil
      @addr = nil
      @logger = nil

      @processor_seq = -1
      @processor_seq_mutex = Mutex.new

      @processors = []
      @processors_mutex = Mutex.new
      @processors_cv = XThread::ConditionVariable.new

      @active_processors = {}
      @active_processors_mutex = Mutex.new
      @active_processors_cv = XThread::ConditionVariable.new
    end

    attr_accessor :id
    attr_accessor :addr
    attr_reader :logger

    attr_reader :processors

    def log_id
      "Node[#{@id}]"
    end
    
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
      Log::info(self, "\t[Powered BY #{RUBY_DESCRIPTION}]") 

      if EXT_FAIRY
	Log::warn self, "\t Load fairy.so"
      else
	Log::warn self, "Can't load fairy.so. Can't use this feature"
      end

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

	pid = NodeAPP.start_subcommand2(CONF.RUBY_BIN, 
					CONF.PROCESSOR_BIN,
					"--node", @deepconnect.local_id.to_s, 
					"--id", processor_id.to_s)
 	Process.wait pid
	
	begin
	  timeout(CONF.SUBCMD_EXEC_TIMEOUT) do
	    while !@processors[processor_id]
	      @processors_cv.wait(@processors_mutex)
	    end
	  end
	rescue Timeout::Error
	  Log::fatal(self, "Can't exec Processor")
	  ERR::Fail ERR::CantExecSubcmd, "processor"
	end

	@master.set_no_of_processors(self, @processors.size)
	@processors[processor_id]
      end
    end

    def terminate_processor(processor)
      deregister_processor(processor)
      begin
	Log::info(self, "terminate processor.")
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
	# ↓パフォーマンス悪い!!
	@processors[processor.id] = processor
	processor.addr = @addr

	@processors_cv.broadcast
     end
    end

    def deregister_processor(processor)
#      @processors.synchronize do

      update_processor_status(processor, :ST_WAIT)

      @processors_mutex.synchronize do
	# ↓パフォーマンス悪い!!
	@processors.delete(processor.id)
	@master.set_no_of_processors(self, @processors.size)

	@processors_cv.broadcast
      end
    end

    #
    # process status management
    #
    def update_processor_status(processor, st)
Log::debug(self, "UPDATE_PROCESSOR_STATUS S: #{processor} #{st}")
      @active_processors_mutex.synchronize do
	case st
	when :ST_WAIT, :ST_SEMIACTIVATE, :ST_FINISH
Log::debug(self, "UPDATE_PROCESSOR_STATUS: 2")
	  if @active_processors.key?(processor)
Log::debug(self, "UPDATE_PROCESSOR_STATUS: 3")
	    @active_processors.delete(processor)
	    @master.set_no_of_active_processors(self, @active_processors.size)
	  end
	when :ST_ACTIVATE
Log::debug(self, "UPDATE_PROCESSOR_STATUS: 4")
	  @active_processors[processor] = processor
	  @master.set_no_of_active_processors(self, @active_processors.size)
	end
      end
Log::debug(self, "UPDATE_PROCESSOR_STATUS: E")
    end

    def to_s
      "#<#{self.class}: #{id}>"
    end
      
    def Node.start(master_host, master_port)
      node = Node.new
      node.start(master_host, master_port)
    end
  end
end


begin
  require "fairy.so"
  EXT_FAIRY = true
rescue LoadError
  EXT_FAIRY = false
end
