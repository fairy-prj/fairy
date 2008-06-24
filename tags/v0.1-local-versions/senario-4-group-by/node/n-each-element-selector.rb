
require "node/n-filter1to1"

module Fairy
  class NEachElementSelector<NFilter1to1
    def initialize(bjob, block_source)
      super(bjob)
      @block_source = block_source
      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
    end

    def start
      super do
	@import.each do |e|
	  if @map_proc.call(e)
	    @export.push e
	  end
	end
	@export.push END_OF_STREAM
      end
    end
  end
end
