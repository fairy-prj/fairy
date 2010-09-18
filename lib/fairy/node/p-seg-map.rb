# encoding: UTF-8

require "fairy/node/p-io-filter"
require "fairy/node/p-single-exportable"

module Fairy
  class PSegMap<PSingleExportFilter
    Processor.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, REF, VAL, VAL)")

    def initialize(id, ntask, bjob, opts, block_source)
      super
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
#      @map_proc = @context.create_proc(@block_source)
    end

#    def start
#      super do
#	@map_proc = BBlock.new(@block_source, @context, self)
#	@map_proc.yield(@import, @export)
#      end
#    end

    def basic_each(&block)
      @map_proc = BBlock.new(@block_source, @context, self)
      @map_proc.yield(@input, block)
    end

    # basic_nextがない

  end
end
