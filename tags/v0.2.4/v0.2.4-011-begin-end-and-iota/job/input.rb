
module Fairy
  module InputInterface
    def input(ffile_descripter, *opts)
      if opts.last.kind_of?(Hash)
	opts_h = opts.pop
      else
	opts_h = {}
      end

      if ffile_descripter.kind_of?(Class)
	ffile_descripter.input(self, opts_h, *opts)
      elsif !ffile_descripter.kind_of?(String) || VFile.vfile?(ffile_descripter)
	FFile.input(self, opts_h, ffile_descripter)
      else
	LFileInput.input(self, opts_h, ffile_descripter)
      end
    end
  end
  def_fairy_interface InputInterface
end

require "job/ffile"
require "job/local-file-input"
require "job/input-iota"
