# encoding: UTF-8

module Fairy
  class NIota<NSingleExportInput
    Processor.def_export self

    def initialize(processor, bjob, opts, first, last)
      super
      @first = first
      @last = last
    end

    def start
      super do
	for i in @first..@last
	  @export.push i
	end
      end
    end

  end
end

