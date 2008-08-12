
require "node/njob"
require "node/n-single-exportable"

module Fairy
  class NEachSubStreamMapper<NFilter
    include NSingleExportable

    def initialize(processor, bjob, block_source, opts=nil)
      super(processor, bjob, opts)
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
      @map_proc = @context.create_proc(@block_source)
    end

    def start
      super do
	@map_proc.call(@import, @export)
      end
    end
  end

end
