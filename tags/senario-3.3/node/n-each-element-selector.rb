
require "node/njob"

module Fairy
  class NEachElementSelector<NJob
    def initialize(block_source)
      @block_source = block_source
      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)

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
	  if @map_proc.call(e)
	    @export_queue.push e
	  end
	end
	@export_queue.push END_OF_STREAM
      end
    end
  end
end
