# encoding: UTF-8

module Fairy

  PORT_BUFFER_SIZE = nil
  PORT_DEFAULT_KEEP_IDENTITY_CLASSES = [
    Binding,
    UnboundMethod,
    Method,
    Proc,
    Dir,
    File,
    IO,
    ThreadGroup,
    Thread,
    Data,
  ]
  if defined?(Continuation)
    PORT_DEFAULT_KEEP_IDENTITY_CLASSES.push Continuation
  end
  if defined?(StopIteration)
    PORT_DEFAULT_KEEP_IDENTITY_CLASSES.push StopIteration
  end
  if defined?(Enumerable::Enumerator)
    PORT_DEFAULT_KEEP_IDENTITY_CLASSES.push Enumerable::Enumerator
  end
  PORT_KEEP_IDENTITY_CLASS_SET = {}
  PORT_DEFAULT_KEEP_IDENTITY_CLASSES.each do|k|
    PORT_KEEP_IDENTITY_CLASS_SET[k] = k
  end

  def self.add_port_keep_identity_class(klass)
    PORT_KEEP_IDENTITY_CLASS_SET[klass] = klass
  end

  DEBUG_PORT_WAIT = CONF.DEBUG_PORT_WAIT

  class Import
    include Enumerable

    Fairy.add_port_keep_identity_class(self)

    END_OF_STREAM = :END_OF_STREAM

    class CTLTOKEN;end
    SET_NO_IMPORT = CTLTOKEN.new

    def initialize(queue = nil)
      if queue
	@queue = queue
      else
	if PORT_BUFFER_SIZE
	  @queue = SizedQueue.new(PORT_BUFFER_SIZE)
	else
	  @queue = Queue.new
	end
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
	  Log::debug(self, "Wait until set @no") if DEBUG_PORT_WAIT
	  @no_cv.wait(@no_mutex)
	  Log::debug(self, "End: Wait until set @no") if DEBUG_PORT_WAIT
	end
	@no
      end
    end

    def no=(no)
      @no_mutex.synchronize do
	@no=no
	@no_cv.broadcast
      end
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
    DeepConnect.def_method_spec(self, "REF push(DVAL)")

    def push_keep_identity(e)
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
    END_OF_STREAM = :END_OF_STREAM

    def initialize(queue = nil)
      @output = nil
      @output_mutex = Mutex.new
      @output_cv = ConditionVariable.new

      if queue
	@queue = queue
      else
	if PORT_BUFFER_SIZE
	  @queue = SizedQueue.new(PORT_BUFFER_SIZE)
	else
	  @queue = Queue.new
	end
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
	  Log::debug(self, "Wait until set @no.") if DEBUG_PORT_WAIT
	  @no_cv.wait(@no_mutex)
	  Log::debug(self, "End: Wait until set @no")  if DEBUG_PORT_WAIT
	end
	@no
      end
    end

    def no=(no)
      @no_mutex.synchronize do
	@no=no
	@no_cv.broadcast
      end
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
	  Log::debug(self, "Wait until set @output") if DEBUG_PORT_WAIT
	  @output_cv.wait(@output_mutex)
	  Log::debug(self, "End: Wait until set @output") if DEBUG_PORT_WAIT
	end
	@output
      end
    end

    def output=(output)
      @output_mutex.synchronize do
	@output = output
	@output_cv.broadcast
      end

      start_export
      nil
    end

    def output_no_import=(n)
      if output?
	@output.no_import = n
      else
	# 遅延設定(shuffleのため)
	Thread.start do
	  Log::debug(self, "@output is nil. Enter delay setting.") if DEBUG_PORT_WAIT
	  output.no_import = n
	  Log::debug(self, "Exit delay setting.") if DEBUG_PORT_WAIT
	end
	n
      end
    end

    def push(e)
      @queue.push e
    end

    def start_export
      Thread.start do
	self.status = :EXPORT
	while (e = @queue.pop) != END_OF_STREAM
	  if PORT_KEEP_IDENTITY_CLASS_SET[e.class]
	    @output.push_keep_identity(e)
	  else
	    @output.push e
	  end
	end
	@output.push END_OF_STREAM
	self.status = END_OF_STREAM
      end
      nil
    end

    def status=(val)
      @status_mutex.synchronize do
	@status = val
	@status_cv.broadcast
      end
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
