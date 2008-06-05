
require "node/njob"

module Fairy
  class NHere<NJob
    def initialize
      @export_queue = SizedQueue.new(10)
    end

    def input=(input)
      @input = input
      start
      self
    end

    def pop
      @export_queue.pop
    end

    def start
      Thread.start do
	while (e = @input.pop) != END_OF_STREAM
	  @export_queue.push e
	end
	@export_queue.push END_OF_STREAM
      end
    end

  end
end
