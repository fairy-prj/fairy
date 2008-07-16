
require "job/job"
require "share/vfile"

module Fairy
  class FFileOutput < Job

    @backend_class = nil

    def FFileOutput.output(fairy, vfn)
      ffile = new(fairy)
      ffile.output(vfn)
      ffile
    end

    def initialize(fairy)
      super

      @vfile = nil
    end

    def backend_class_name
      "BFileOutput"
    end

    def output(vfn)
      @descripter = vfn
      @vfile = VFile.new
      @vfile.vfile_name = vfn
      backend.output(@vfile)

#      vf.create_vfile(vfn)
    end

    def input=(job)
      @input = job
      backend.input=job.backend

      backend.wait_all_output_finished
      @vfile.create_vfile
    end
  end

end
