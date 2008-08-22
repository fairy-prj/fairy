
module Fairy

  PORT_BUFFER_SIZE = 10

  class Import
    include Enumerable

    END_OF_STREAM = :END_OF_STREAM

    class CTLTOKEN;end
    SET_NO_IMPORT = CTLTOKEN.new

    def initialize(queue = nil)
      if queue
	@queue = queue
      else
	@queue = SizedQueue.new(PORT_BUFFER_SIZE)
      end

      @no = nil
      @no_mutex = Mutex.new
      @no_cv = ConditionVariable.new

      @key = nil

      @no_import = nil
      @no_eos = 0
    end

    def no
      @no_mutex.synchronize do
	while !@no
	  @no_cv.wait(@no_mutex)
	end
	@no
      end
    end

    def no=(no)
      @no=no
      @no_cv.broadcast
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
    # 取りあえず
#    DeepConnect.def_method_spec(self, "REF push(DVAL)")

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
    END_OF_STREAM = :END_OF_STREAM

    def initialize(queue = nil)
      @output = nil
      @output_mutex = Mutex.new
      @output_cv = ConditionVariable.new

      if queue
	@queue = queue
      else
	@queue = SizedQueue.new(PORT_BUFFER_SIZE)
      end

      @no = nil
      @no_mutex = Mutex.new
      @no_cv = ConditionVariable.new

      @key = nil

      @status = nil
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new
    end

    def no
      @no_mutex.synchronize do
	while !@no
	  @no_cv.wait(@no_mutex)
	end
	@no
      end
    end

    def no=(no)
      @no=no
      @no_cv.broadcast
    end

    attr_reader :key
    def add_key(key)
      @key = key
    end

    def output?
      @output_mutex.synchronize do
	@output
      end
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
      nil
    end

    def output_no_import=(n)
      if output?
	@output.no_import = n
      else
	# 遅延設定(shuffleのため)
	Thread.start do
	  output.no_import = n
	end
      end
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
