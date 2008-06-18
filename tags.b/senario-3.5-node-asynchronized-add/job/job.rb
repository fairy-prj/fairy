
require "thread"

module Fairy
  class Job
    def initialize(fairy, *rests)
      @fairy = fairy
      atom = Atom.new(backend_class, :new, fairy.backend_controller, *rests)
      p atom
      @ref = @fairy.send_atom(atom)
    end

    def backend
      @ref.value
    end

    def backend=(v)
      @ref.value= v
    end

    def map(block_source)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachElementMapper.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def smap(block_source)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachSubStreamMapper.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def select(block_source)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachElementSelector.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def grep(regexp)
      select %{|e| /#{regexp.source}/ === e}
    end

    def here
      here = Here.new(@fairy)
      here.input= self
      here
    end

    def group_by
      group_by = GroupBy(@fairy)
      group_by.input = self
      group_by
    end
  end
end

require "job/each-element-mapper"
require "job/each-substream-mapper"
require "job/each-element-selector"
require "job/here"
require "job/group-by"
