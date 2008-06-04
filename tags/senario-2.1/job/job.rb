
require "thread"

module Fairy
  class Job
    def initialize(fairy, *rests)
      @fairy = fairy
      atom = Atom.new(backend_class, :new, fairy.backend_controller, *rests)
      @ref = @fairy.send_atom(atom)
    end

    def backend
      @ref.value
    end

    def backend=(v)
      @ref.value= v
    end

    def grep(regexp)
      mapper = EachElementMapper.new(@fairy, %{|e| /#{regexp.source}/ === e})
      mapper.input=self
      mapper
    end

  end
end

require "job/each-element-mapper"
