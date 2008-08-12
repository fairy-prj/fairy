module Fairy
  
  module OutputInterface
    def output(vfn, opts = nil)
      if vfn.kind_of?(Class)
	outputter = vfn.output(@fairy, vfn, opts)
      elsif !vfn.kind_of?(String) || VFile.vfile?(vfn)
	outputter = FFileOutput.output(@fairy, vfn, opts)
      else
	outputter = LFileOutput.output(@fairy, vfn, opts)
      end
      outputter.input = self
      outputter

    end
  end
  def_job_interface OutputInterface

end

require "job/ffile-output"
require "job/local-file-output"


