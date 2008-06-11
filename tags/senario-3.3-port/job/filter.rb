
require "job/job"

module Fairy
  class Filter < Job

    def initialize(fairy, *rests)
      super
    end

    def backend_class
      error
    end

    def input=(job)
      @input=job
      atom = Atom.new(backend, :input=, job.backend)
      @fairy.send_atom(atom)
    end
  end
end
