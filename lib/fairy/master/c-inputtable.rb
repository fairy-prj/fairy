# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  module CInputtable

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
