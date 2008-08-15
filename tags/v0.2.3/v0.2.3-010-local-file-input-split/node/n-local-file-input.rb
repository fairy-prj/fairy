
require "node/njob"
require "node/port"
require "node/n-single-exportable"

module Fairy
  class NLFileInput<NJob
    include NSingleExportable

    def self.open(processor, bjob, io)
      nlfileinput = self.new(processor, bjob)
      nlfileinput.open(job)
    end

    def initialize(processor, bjob)
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
	  @export.push l
	end
      end
    end
  end
end
