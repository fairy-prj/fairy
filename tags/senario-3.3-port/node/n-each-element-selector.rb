
require "node/n-filter1to1"

module Fairy
  class NEachElementSelector<NFilter1to1
    def initialize(block_source)
      super()
      @block_source = block_source
      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
    end

    def start
      Thread.start do
	@import.each do |e|
#	  puts "IMPORT: #{e.inspect}"
	  if @map_proc.call(e)
#	    puts "EXPORT: #{e.inspect}"
	    @export.push e
	  end
	end
	@export.push END_OF_STREAM
      end
    end
  end
end
