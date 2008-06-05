
require "job/job"
require "backend/atom"
module Fairy
  class FFile < Job

    def FFile.open(fairy, ffile_descripter)
      ffile = new(fairy)
      ffile.open(ffile_descripter)

    end

    def FFile.input(fairy, ffile_descripter)
      FFile.open(fairy, ffile_descripter)
    end

    def initialize(fairy)
      super
    end

    def backend_class
      BFile
    end

    def open(ffile_descripter)
      @descripter = ffile_descripter
      atom = Atom.new(backend, :open, @descripter)
      @fairy.send_atom(atom)
      self
    end
  end

  class BFile;end
end
