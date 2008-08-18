
require "job/job"
require "share/varray"

module Fairy
  class OutputVArray<Job
    
    def self.output(fairy, opts)
      output = new(fairy, opts)
      output
    end

    def initialize(fairy, opts=nil)
      super

      @varray = backend.varray
    end

    attr_reader :varray

    def backend_class_name
      "BOutputVArray"
    end

    def output
      backend.output
    end

    def input=(job)
      @input = job
      backend.input=job.backend
      backend.wait_all_output_finished
    end
  end
end
