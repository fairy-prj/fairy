# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
require "fairy/node/p-single-exportable"

module Fairy
  class PSelect<PSingleExportFilter
    Processor.def_export self

    def initialize(id, ntask, bjob, opts, block_source)
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
