# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/n-single-exportable"

module Fairy
  class NBarrierMemoryBuffer<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts=nil)
      @export = Export.new(CONF.BARRIER_MEMORY_BUFFERING_POLICY)
      super
    end

    def input=(input)
      unless @import
	@import = Import.new(Queue.new)
	@import.no=input.no
	@import.add_key(input.key)
	@import.set_log_callback do |n| 
	  Log::verbose(self, "IMPORT POP: #{n}")
	end

	start
      end
      self
    end

    def start
      super do
	@bjob.wait_export
	@import.each{|e| @export.push e}
      end
    end
  end

end
