# encoding: UTF-8

require "fairy/node/n-filter"
require "fairy/node/n-single-exportable"

module Fairy
  class NHere<NFilter
    Processor.def_export self

    include NSingleExportable

#     def input=(input)
#       super
#     end

#     def start
#       super do
# 	@input.each do |e|
# # 	  if e.__deep_connect_reference? && e.kind_of?(Array)
# # 	    e = e.to_a
# # 	  end
# 	  @export.push e
# 	end
#       end
#     end

    def basic_each(&block)
      @input.each do |e|
	block.call e
      end
    end
  end
end
