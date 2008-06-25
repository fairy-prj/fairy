
require "deep-connect/deep-connect.rb"

require "backend/job-interpriter"
require "backend/scheduler"

module Fairy

  class Controller
    
    def initialize
      @job_interpriter = JobInterpriter.new(self)
      @scheduler = Scheduler.new(self)
    end

    def send_atom(atom)
      @job_interpriter.exec(atom)
    end

    #
    # BEGIN DFRQ
    # * �����ӥ���Ω���夲
    #
    def start(service)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Controller", self)
      @deepconnect.register_service("BFile", BFile)
      @deepconnect.register_service("BHere", BHere)
    end

    def Controller.start(service)
      controller = Controller.new
      controller.start(service)
    end
    #
    # END DFRQ
    #
  end
end
