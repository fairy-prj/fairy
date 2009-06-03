# encoding: UTF-8

require "fairy/share/vfile"

module Fairy
  class LFileOutput < Job

    @backend_class = nil

    def LFileOutput.output(fairy, opts, filename)
      ffile = new(fairy, opts)
      ffile.output(filename)
      ffile
    end

    def initialize(fairy, opts=nil)
      super
      @opts = opts
      
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
	backend.each_buf do |buf|
	  buf.each do |l|
	    io.puts l
	  end
	end
      end
    end
  end
end
