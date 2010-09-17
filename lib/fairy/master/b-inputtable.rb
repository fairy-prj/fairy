# encoding: UTF-8

module Fairy
  module BInputtable

    def initialize(*rests)
      super
      @input = nil
    end

    def input=(input)
      @input = input

      start_create_nodes
    end

    attr_reader :input

    def inputtable?
      true
    end

    def break_running(njob = nil)
      super
      Thread.start{@input.break_running}
    end
  end
end
