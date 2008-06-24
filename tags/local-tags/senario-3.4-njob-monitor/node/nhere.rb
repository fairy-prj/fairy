
require "node/njob"

module Fairy
  class NHere<NFilter1to1
    def start
      super do
	@import.each do |e|
#	  puts "NHere::Import #{e}"
	  @export.push e
	end
	@export.push END_OF_STREAM
      end
    end

    def output=(output)
      @export.output= output
    end
  end
end
