
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  class NEachElementMapper<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
#      @map_proc = @context.create_proc(@block_source)
      @map_proc = BBlock.new(@block_source, @context, self)
    end

    def start
      super do
	@import.each do |e|
	  @export.push @map_proc.yield(e)
	end
      end
    end
  end
end
