
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
      @deepconnect.export("Controller", self)

      @deepconnect.export("BJob", BJob)
      @deepconnect.export("BFile", BFile)
      @deepconnect.export("BHere", BHere)
      @deepconnect.export("BEachElementMapper", BEachElementMapper)
      @deepconnect.export("BEachElementSelector", BEachElementSelector)
      @deepconnect.export("BEachSubStreamMapper", BEachSubStreamMapper)
      @deepconnect.export("BGroupBy", BGroupBy)
      @deepconnect.export("BZipper", BZipper)
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

    def assign_group_by_processor
      # プロセスを新規に立ち上げるので同じになる
      assign_input_processor
    end

    def register_processor(processor)
      @processors_mutex.synchronize do
	@processors[processor.id] = processor
	@processors_cv.broadcast
      end
    end

    def assign_inputtable_processor(bjob, njob, export)
      case bjob
      when BGroupBy
	assign_group_by_processor
      else
	njob.processor
      end
    end
    #
    # END DFRQ
    #

  end
end
