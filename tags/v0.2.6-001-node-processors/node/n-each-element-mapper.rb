
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  class NEachElementMapper<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
      @map_proc = @context.create_proc(@block_source)
    end

    def start
      super do
	@import.each do |e|
	  begin
	    if e.__deep_connect_reference? && e.kind_of?(Array)
	      e = e.to_a
	    end
	    if @map_proc.respond_to?(:yield)
	      @export.push @map_proc.yield(e)
	    else
	      if @map_proc.arity == 1 
		@export.push @map_proc.call(e)
	      else
		@export.push @map_proc.call(*e)
	      end
	    end
	  rescue Exception
	    p $!, $@
	  end
	end
      end
    end
  end
end
