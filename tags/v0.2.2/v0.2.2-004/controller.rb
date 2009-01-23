
require "deep-connect/deep-connect.rb"

#require "backend/job-interpriter"
#require "backend/scheduler"

require "backend/bfile"
require "backend/b-each-element-mapper"
require "backend/b-each-substream-mapper"
require "backend/b-each-element-selector"
require "backend/bhere"
require "backend/b-group-by"
require "backend/b-zipper"

module Fairy

  class Controller
    
    def initialize(id)
      @id = id
#      @job_interpriter = JobInterpriter.new(self)
#      @scheduler = Scheduler.new(self)

#      @processors = []

      @services = {}
    end

    attr_reader :id

#     def send_atom(atom)
#       @job_interpriter.exec(atom)
#     end

    def start(master_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Controller", self)
      export("BJob", BJob)
      export("BFile", BFile)
      export("BHere", BHere)
      export("BEachElementMapper", BEachElementMapper)
      export("BEachElementSelector", BEachElementSelector)
      export("BEachSubStreamMapper", BEachSubStreamMapper)
      export("BGroupBy", BGroupBy)
      export("BZipper", BZipper)

      @master_deepspace = @deepconnect.open_deepspace("localhost", master_port)
      @master = @master_deepspace.import("Master")
      @master.register_controller(self)
    end

    def export(service, obj)
      @services[service] = obj
    end

    def import(service)
      @services[service]
    end

    #
    # BEGIN DFRQ
    # * Input Processorの割り当て
    # 
    def assign_input_processor(host)
      processor  = @master.assign_processor(:INPUT, host)
    end

    def assign_inputtable_processor(input_bjob, njob, export)
      case input_bjob
      when BGroupBy
	@master.assign_processor(:NEW_PROCESSOR)
      else
puts "NJOB: #{njob.processor}"
	@master.assign_processor(:SAME_PROCESSOR, njob.processor)
      end
    end
    #
    # END DFRQ
    #
    def Controller.start(id, master_port)
      controller = Controller.new(id)
      controller.start(master_port)
    end

  end
end
