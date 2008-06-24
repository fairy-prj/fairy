
require "node/njob"

module Fairy
  class NEachSubStreamMapper<NJob
    def initialize(block_source)
      @block_source = block_source
      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)

      @import_stream = nil
      @export_stream = ExportStream.new
    end

    def input=(input)
#      @input = input
      @import_stream = ImportStream.new(input)
      start
      self
    end

    def pop
      @export_stream.pop
    end

    def start
      Thread.start do
	@map_proc.call(@import_stream, @export_stream)
      end
    end
  end

  class ImportStream
    include Enumerable

    END_OF_STREAM = NJob::END_OF_STREAM

    def initialize(input)
      @input = input
    end

    def each(&block)
      while (e = @input.pop) != END_OF_STREAM
	block.call(e)
      end
    end
  end

  class ExportStream
    include Enumerable

    END_OF_STREAM = NJob::END_OF_STREAM

    def initialize
      @queue = SizedQueue.new(10)
    end

    def push(e)
      @queue.push e
    end

    def push_eos
      @queue.push END_OF_STREAM
    end

    def pop
      @queue.pop
    end

    def each(&block)
      while (e = @input.pop) != END_OF_STREAM
	block.call(e)
      end
    end
  end
end
