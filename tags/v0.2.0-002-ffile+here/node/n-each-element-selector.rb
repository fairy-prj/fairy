
require "node/n-filter"

module Fairy
  class NEachElementSelector<NFilter
    include NSingleExportable

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
      end
    end
  end
end
