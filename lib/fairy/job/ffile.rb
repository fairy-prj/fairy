# encoding: UTF-8

require "fairy/job/job"
require "fairy/share/vfile"

module Fairy
  class FFile < Job

    @backend_class = nil

    def FFile.open(fairy, opts, ffile_descripter)
      ffile = new(fairy, opts)
      ffile.open(ffile_descripter)
      ffile
    end

    def FFile.input(fairy, opts, ffile_descripter)
      FFile.open(fairy, opts, ffile_descripter)
    end

    def initialize(fairy, opts=nil)
      super
    end

    def backend_class_name
      "BFile"
    end

    def open(ffile_descripter)
      @descripter = ffile_descripter
      case ffile_descripter
      when Array
	vf = VFile.real_files(ffile_descripter)
      when VFile
	vf = ffile_descripter
      when String
	if VFile.vfile?(ffile_descripter)
	  vf = VFile.vfile(ffile_descripter)
	else
	  vf = VFile.real_files([ffile_descripter])
	end
      else
	raise "指定が間違っています"
      end
      backend.open(vf)
      self
    end

  end

#  class BFile;end
end
