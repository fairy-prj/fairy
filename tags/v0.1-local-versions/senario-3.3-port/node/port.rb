module Fairy
  class Import
    include Enumerable

    END_OF_STREAM = NJob::END_OF_STREAM

    def initialize
      @queue = SizedQueue.new(10)
    end

    def push(e)
      @queue.push e
    end

    def each(&block)
      while (e = @queue.pop) != END_OF_STREAM
	block.call(e)
      end
    end
  end

  class Export
    END_OF_STREAM = NJob::END_OF_STREAM

    def initialize
      @output = nil
      @queue = SizedQueue.new(10)
    end

    def output=(output)
      @output = output
      start_export
    end

    def push(e)
      @queue.push e
    end

    def push_eos
      @queue.push END_OF_STREAM
    end

    def start_export
      Thread.start do
	while (e = @queue.pop) != END_OF_STREAM
	  @output.push e
	end
	@output.push END_OF_STREAM
      end
    end
  end
end
