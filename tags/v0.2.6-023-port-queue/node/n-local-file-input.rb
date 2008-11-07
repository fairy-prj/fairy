
require "node/njob"
require "node/n-single-exportable"

module Fairy
  class NLFileInput<NSingleExportInput
    Processor.def_export self

    def self.open(processor, bjob, io, opts=nil)
      nlfileinput = self.new(processor, bjob, opts)
      nlfileinput.open(job)
    end

    def initialize(processor, bjob, opts=nil)
      super
    end

    def open(io)
      @io = io
      start
      self
    end

    def start
      super do
	for l in @io
#Log::debug(self, l)
	  @export.push l
	end
      end
    end
  end
end
