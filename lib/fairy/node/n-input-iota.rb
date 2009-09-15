# encoding: UTF-8

require "fairy/node/n-single-exportable"

module Fairy
  class NIota<NSingleExportInput
    Processor.def_export self

    def initialize(processor, bjob, opts)
      super
    end

    def open(niota_place)
      self.no = niota_place.no

      @first = niota_place.first
      @last = niota_place.last
    end

    def basic_each(&block)
      for i in @first..@last
	block.call i
      end
    end

#     def start
#       super do
# 	for i in @first..@last
# 	  @export.push i
# 	end
#       end
#     end

  end
end

