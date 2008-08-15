
require "node/n-filter"

module Fairy
  class NEachElementSelector<NFilter
    Processor.def_export self

    include NSingleExportable

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
      @map_proc = @context.create_proc(@block_source)
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
