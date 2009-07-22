# encoding: UTF-8

module Fairy
  class NInputVArray<NSingleExportInput
    Processor.def_export self

#    def initialize(processor, bjob, opts)
#      super
#    end

    def open(nvarrayplace)
      @array = nvarrayplace.ary
      self.no = nvarrayplace.no
Log::debug(self, "INPUT_VARRAY: no= #{@no}")

    end

    def basic_each(&block)
Log::debug(self, "INPUT_VARRAY:S")
      @array.each &block
Log::debug(self, "INPUT_VARRAY:E")
    end
  end
end

