# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/filter"
require "fairy/share/vfile"

module Fairy
  class OutputFile < Filter

    def OutputFile.output(fairy, opts, vfn)
      ffile = new(fairy, opts)
      ffile.output(vfn)
      ffile
    end

    def initialize(fairy, opts=nil)
      super

      @old_vfile = nil
      @vfile = nil
    end

    def backend_class_name
      "COutputFile"
    end

    def output(vfn)
      if File.exists?(vfn)
	@old_vfile = VFile.vfile(vfn)
      end

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

      if @old_vfile
	rmfiles = @old_vfile.real_file_names.zip(@vfile.real_file_names).select{|n1, n2| n1 != n2}.map{|n1, n2| n1}
	if !rmfiles.empty?
	  rm = @fairy.exec(rmfiles).map(%{|uri|
             path = URI(uri).path
             begin
               File.unlink(path)
             rescue
             end
             }, :BEGIN => %{require "uri"})
	  rm.done
	end
      end
      @vfile.create_vfile
    end
  end

end
