# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/filter"
require "fairy/share/vfile"

module Fairy
  class OutputFile < Filter

    @backend_class = nil

    def OutputFile.output(fairy, opts, vfn)
      ffile = new(fairy, opts)
      ffile.output(vfn)
      ffile
    end

    def initialize(fairy, opts=nil)
      super

      @vfile = nil
    end

    def backend_class_name
      "COutputFile"
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
