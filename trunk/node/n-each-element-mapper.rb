
module Fairy
  class NEachElementMapper
    def initialize(block_source)
      @block_source = block_source
      @map_proc = eval("proc {#{@block_source}}", TOPLEVEL_BINDING)

      @export_queue = SizedQueue.new(10)
    end

    def input=(input)
      @input = input
      start
      self
    end

    def start
      Thread.start do
	while e = @input.pop
	  @export_queue.push e
	end
	@export_queue.push nil
      end
    end
  end
end
