
require "node/njob"
require "node/n-single-exportable"

module Fairy
  class NBarrierMemoryBuffer<NFilter
    Processor.def_export self

    include NSingleExportable

    def initialize(processor, bjob, opts=nil)
      @export = Export.new(Queue.new)
      super
    end

    def input=(input)
      unless @import
	@import = Import.new(Queue.new)
	@import.no=input.no
	@import.add_key(input.key)
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
