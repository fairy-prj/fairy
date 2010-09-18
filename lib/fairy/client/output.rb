# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  
  module OutputInterface
    def output(vfn, opts = nil)
      if vfn.kind_of?(Class)
	outputter = vfn.output(@fairy, opts)
      elsif !vfn.kind_of?(String) || VFile.vfile?(vfn)
	outputter = OutputFile.output(@fairy, opts, vfn)
      else
	outputter = OutputLocalFile.output(@fairy, opts, vfn)
      end
      outputter.input = self
      outputter

    end
  end
  def_filter_interface OutputInterface

end

require "fairy/client/output-file"
require "fairy/client/output-local-file"


