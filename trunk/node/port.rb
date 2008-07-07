
require "node/njob"

module Fairy
  class Import
    include Enumerable

    END_OF_STREAM = NJob::END_OF_STREAM

    class CTLTOKEN;end
    SET_NO_IMPORT = CTLTOKEN.new

    def initialize
      @queue = SizedQueue.new(10)

      @key = nil

      @no_import = nil
      @no_eos = 0
    end

    attr_reader :key
    def add_key(key)
      @key = key
    end

    def no_import=(n)
      @no_import = n
      @queue.push SET_NO_IMPORT
    end

    def push(e)
      @queue.push e
    end

    def pop
      while !@no_import or @no_import > @no_eos
	case e = @queue.pop
	when SET_NO_IMPORT
	  # do nothing
	when END_OF_STREAM
	  @no_eos += 1
	else
	  return e
	end
      end
      return nil
    end

    def each(&block)
      while !@no_import or @no_import > @no_eos
	case e = @queue.pop
	when SET_NO_IMPORT
	  # do nothing
	when END_OF_STREAM
	  @no_eos += 1
	else
	  block.call(e)
	end
      end
    end

    def size
      size = 0
      each{size += 1}
      size
    end

  end

  class Export
    END_OF_STREAM = NJob::END_OF_STREAM

    def initialize
      @output = nil
      @output_mutex = Mutex.new
      @output_cv = ConditionVariable.new

      @queue = SizedQueue.new(10)

      @key = nil

      @status = nil
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new
    end

    attr_reader :key
    def add_key(key)
      @key = key
    end

    def output
      @output_mutex.synchronize do
	while !@output
	  @output_cv.wait(@output_mutex)
	end
	@output
      end
    end

    def output=(output)
      @output = output
      @output_cv.broadcast

      start_export
    end

    def push(e)
      @queue.push e
    end

    def start_export
      Thread.start do
	self.status = :EXPORT
	while (e = @queue.pop) != END_OF_STREAM
	  @output.push e
	end
	@output.push END_OF_STREAM
	self.status = END_OF_STREAM
      end
    end

    def status=(val)
#      @status_mutex.synchronize do
	@status = val
	@status_cv.broadcast
#      end
    end

    def wait_finish
      @status_mutex.synchronize do
	while @status != END_OF_STREAM
	  @status_cv.wait(@status_mutex)
	end
	@status = :EXPORT_FINISH
      end
    end
  end
end
