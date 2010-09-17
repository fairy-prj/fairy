# encoding: UTF-8

require "fairy/node/n-single-exportable"

module Fairy
  class NInputVArray<NSingleExportInput
    Processor.def_export self

#    def initialize(processor, bjob, opts)
#      super
#    end

    def open(nvarrayplace)
      @array = nvarrayplace.ary
      self.no = nvarrayplace.no
    end

    def basic_each(&block)
      @array.each &block
    end
  end
end

