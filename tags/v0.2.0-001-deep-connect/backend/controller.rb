
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

    def start(service)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Controller", self)
    end

    def Controller.start(service)
      controller = Controller.new
      controller.start(service)
    end
  end
end
