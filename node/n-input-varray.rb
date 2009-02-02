# encoding: UTF-8

module Fairy
  class NInputVArray<NSingleExportInput
    Processor.def_export self

    def initialize(processor, bjob, opts, array)
      super
      @array = array
    end

    def start
      super do
	for i in @array
	  @export.push i
	end
      end
    end

  end
end

