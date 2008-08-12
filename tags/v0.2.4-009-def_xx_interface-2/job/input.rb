
module Fairy
  module InputInterface
    def input(ffile_descripter, opts = nil)
      if !ffile_descripter.kind_of?(String) || VFile.vfile?(ffile_descripter)
	FFile.input(self, ffile_descripter)
      else
	LFileInput.input(self, ffile_descripter, opts)
      end
    end
  end
  def_fairy_interface InputInterface
end

require "job/ffile"
require "job/local-file-input"
