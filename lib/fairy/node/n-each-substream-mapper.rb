# encoding: UTF-8

require "node/njob"
require "node/n-single-exportable"

module Fairy
  class NEachSubStreamMapper<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
#      @map_proc = @context.create_proc(@block_source)
    end

    def start
      super do
	@map_proc = BBlock.new(@block_source, @context, self)
	@map_proc.yield(@import, @export)
      end
    end
  end

end
