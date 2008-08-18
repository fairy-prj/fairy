
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  class NEachElementMapper<NFilter
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
	  begin
	  @export.push @map_proc.call(e)
	  rescue Exception
	    p $!, $@
	  end
	end
      end
    end
  end
end
