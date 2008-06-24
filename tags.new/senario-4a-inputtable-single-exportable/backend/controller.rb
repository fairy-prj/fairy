
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
    
  end

end
