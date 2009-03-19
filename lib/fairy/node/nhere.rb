# encoding: UTF-8

require "fairy/node/n-filter"
require "fairy/node/n-single-exportable"

module Fairy
  class NHere<NFilter
    Processor.def_export self

    include NSingleExportable

    def start
      super do
	@import.each do |e|
# 	  if e.__deep_connect_reference? && e.kind_of?(Array)
# 	    e = e.to_a
# 	  end
	  @export.push e
	end
      end
    end
  end
end
