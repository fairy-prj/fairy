# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/filter"
require "fairy/share/vfile"

module Fairy
  class InputFile<Filter

    def InputFile.open(fairy, opts, ffile_descripter)
      ffile = new(fairy, opts)
      ffile.open(ffile_descripter)
      ffile
    end

    def InputFile.input(fairy, opts, ffile_descripter)
      InputFile.open(fairy, opts, ffile_descripter)
    end

    def initialize(fairy, opts=nil)
      super
    end

    def backend_class_name
      "CInputFile"
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
	ERR::Raise ERR::IllegalVFile
      end
      backend.open(vf)
      self
    end

  end

#  class BFile;end
end
