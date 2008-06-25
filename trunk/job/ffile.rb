
require "job/job"
#require "backend/atom"
module Fairy
  class FFile < Job

    @backend_class = nil

    def FFile.open(fairy, ffile_descripter)
      ffile = new(fairy)
      ffile.open(ffile_descripter)
      ffile
    end

    def FFile.input(fairy, ffile_descripter)
      FFile.open(fairy, ffile_descripter)
    end

    def initialize(fairy)
      super
    end

    def backend_class_name
      "BFile"
    end

    def open(ffile_descripter)
      @descripter = ffile_descripter
#      atom = Atom.new(backend, :open, @descripter)
#      @fairy.send_atom(atom)
      backend.open(@descripter)
      self
    end
  end

#  class BFile;end
end
