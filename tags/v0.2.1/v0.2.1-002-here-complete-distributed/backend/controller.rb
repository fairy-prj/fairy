
require "deep-connect/deep-connect.rb"

require "backend/job-interpriter"
require "backend/scheduler"

module Fairy

  class Controller
    
    def initialize
      @job_interpriter = JobInterpriter.new(self)
      @scheduler = Scheduler.new(self)

      @processors = []
      @processors_mutex = Mutex.new
      @processors_cv = ConditionVariable.new
    end

    def send_atom(atom)
      @job_interpriter.exec(atom)
    end

    #
    # BEGIN DFRQ
    # * サービスの立ち上げ
    #
    def start(service)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Controller", self)

      @deepconnect.register_service("BJob", BJob)
      @deepconnect.register_service("BFile", BFile)
      @deepconnect.register_service("BHere", BHere)
      @deepconnect.register_service("BEachElementMapper", BEachElementMapper)
      @deepconnect.register_service("BEachElementSelector", BEachElementSelector)
      @deepconnect.register_service("BEachSubStreamMapper", BEachSubStreamMapper)
      @deepconnect.register_service("BGroupBy", BGroupBy)
      @deepconnect.register_service("BZipper", BZipper)
    end

    def Controller.start(service)
      controller = Controller.new
      controller.start(service)
    end
    #
    # END DFRQ
    #

    #
    # BEGIN DFRQ
    # * Input Processorの割り当て
    # 
    def assign_input_processor
      @processors_mutex.synchronize do
	processor_id = @processors.size
#	Process.spawn("test/testn.rb", 
#		      "--controller", @deepconnect.local_id, 
#		      "--id", processor_id.to_s)
	Process.fork do
	  exec("test/testn.rb", 
	       "--controller", @deepconnect.local_id.to_s, 
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
	@processors_cv.broadcast
      end
    end

    def assign_inputtable_processor(bjob, njob, export)
      p = njob.processor
      p
    end
    #
    # END DFRQ
    #

  end
end
