# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/port"

module Fairy
  class NFilter<NJob
    Processor.def_export self

    ST_WAIT_IMPORT = :ST_WAIT_IMPORT

    def initialize(processor, bjob, opts=nil, *rests)
      super
      self.status = ST_WAIT_IMPORT
    end

    def input=(input)
      @input = input
      self.no = input.no
      self.key = input.key
    end
  end
end
