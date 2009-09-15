# encoding: UTF-8

require "fairy/node/n-filter"
require "fairy/node/n-single-exportable"

module Fairy
  class NEachElementSelector<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
    end

#     def start
#       super do
# 	@map_proc = BBlock.new(@block_source, @context, self)
# 	@import.each do |e|
# 	  if @map_proc.yield(e)
# 	    @export.push e
# 	  end
# 	end
#       end
#     end

    def basic_each(&block)
      @map_proc = BBlock.new(@block_source, @context, self)
      @input.each do |e|
	if @map_proc.yield(e)
	  block.call e
	end
      end
    end
  end
end
