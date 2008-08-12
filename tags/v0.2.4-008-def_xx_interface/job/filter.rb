
require "job/job"

module Fairy
  class Filter < Job

    def initialize(fairy, *rests)
      super
    end

    def backend_class_name
      raise "backend_classの名前を定義してください"
    end

    def input=(job)
      @input=job
#      atom = Atom.new(backend, :input=, job.backend)
#      @fairy.send_atom(atom)
      backend.input=job.backend
    end
  end
end
