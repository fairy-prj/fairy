
require "job/job"
require "share/vfile"

module Fairy
  class LFileOutput < Job

    @backend_class = nil

    def LFileOutput.output(fairy, filename)
      ffile = new(fairy)
      ffile.output(filename)
      ffile
    end

    def initialize(fairy)
      super

      @filename = nil
    end

    def backend_class_name
      "BLFileOutput"
    end

    def output(filename)
      @filename = filename
      backend.output(self)

    end

    def input=(job)
      @input = job
      backend.input=job.backend
      
      File.open(@filename, "w") do |io|
	for l in backend
	  io.puts l
	end
      end
    end
  end
end
