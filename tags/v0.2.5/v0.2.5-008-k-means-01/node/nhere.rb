
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  class NHere<NFilter
    Processor.def_export self

    include NSingleExportable

    def start
      super do
	@import.each do |e|
	  @export.push e
	end
      end
    end
  end
end
