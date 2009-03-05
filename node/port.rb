# encoding: UTF-8

require "forwardable"

module Fairy

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
    class CTLTOKEN_SET_NO_IMPORT<CTLTOKEN;end
    SET_NO_IMPORT = CTLTOKEN_SET_NO_IMPORT.new

    def initialize(policy = nil)

      @queuing_policy = policy
      @queuing_policy ||= CONF.POSTQUEUING_POLICY

      case @queuing_policy
      when Hash
	@queue = eval("#{@queuing_policy[:queuing_class]}").new(@queuing_policy)
      else
	@queue = @queuing_policy
      end

      @log_import_ntimes_pop = CONF.LOG_IMPORT_NTIMES_POP

      @no = nil
      @no_mutex = Mutex.new
      @no_cv = ConditionVariable.new

      @key = nil

      @no_import = nil

      @no_pop_elements = 0
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
	when CTLTOKEN_SET_NO_IMPORT
	  # do nothing
	when END_OF_STREAM
	  @no_eos += 1
	else
	  @no_pop_elements += 1
	  if @log_import_ntimes_pop && 
	      @no_pop_elements % @log_import_ntimes_pop == 0
	    Log::info(self, "INPORT POP: #{@no_pop_elements}")
	  end
	  return e
	end
      end
      return nil
    end

    def each(&block)
      while !@no_import or @no_import > @no_eos
	case e = @queue.pop
	when CTLTOKEN_SET_NO_IMPORT
	  # do nothing
	when END_OF_STREAM
	  @no_eos += 1
	else
	  @no_pop_elements += 1
	  if @log_import_ntimes_pop &&
	      @no_pop_elements % @log_import_ntimes_pop == 0
	    Log::info(self, "INPORT POP: #{@no_pop_elements}")
	  end
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

    def initialize(policy = nil)
      
      @queuing_policy = policy
      @queuing_policy ||= CONF.POSTQUEUING_POLICY

      @output = nil
      @output_mutex = Mutex.new
      @output_cv = ConditionVariable.new

      case @queuing_policy
      when Hash
	@queue = eval("#{@queuing_policy[:queuing_class]}").new(@queuing_policy)
      else
	@queue = @queuing_policy
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

  module OnMemoryQueue
    def self.new(policy)
      Queue.new
    end
  end

  class OnMemeorySizedQueue
    extend Forwardable

    def initialize(policy)
      size = policy[:size]
      size ||= CONF.ONMEMORY_SIZEDQUEUE_SIZE
      @queue = SizedQueue.new(size)
    end

    def_delegator :@queue, :push
    def_delegator :@queue, :pop
  end

  class FileBufferdQueue
    def initialize(policy)
      @policy = policy
      @threshold = policy[:threshold]
      @threshold ||= CONF.FILEBUFFEREDQUEUE_THRESHOLD

      @push_queue = []
      @pop_queue = @push_queue
      @buffers_queue = nil

      @queue_mutex = Mutex.new
      @queue_cv = ConditionVariable.new
    end

    def push(e)
      @queue_mutex.synchronize do
	@push_queue.push e
	@queue_cv.signal
	if @push_queue.size >= @threshold
	  if @push_queue.equal?(@pop_queue)
	    @push_queue = []
	  else
	    store_2ndmemory(@push_queue)
	    @push_queue = []
	  end
	end
      end
    end

    def pop
      @queue_mutex.synchronize do
	while @pop_queue.empty?
	  if @pop_queue.equal?(@push_queue)
	    @queue_cv.wait(@queue_mutex)
	  elsif @buffers_queue.nil?
	    @pop_queue = @push_queue
	  elsif @buffers_queue.empty?
	    @pop_queue = @push_queue
	    @push_queue = []
	    @buffers_queue = nil
	  else
	    @pop_queue = restore_2ndmemory
	  end
	end
	@pop_queue.shift
      end
    end

    def init_2ndmemory
      require "tempfile"

      @buffer_dir = @policy[:buffer_dir]
      @buffer_dir ||= CONF.TMP_DIR

      @buffers_queue = Queue.new
    end

    def open_2ndmemory(&block)
      unless @buffers_queue
	init_2ndmemory
      end
      buffer = Tempfile.open("port-buffer-", @buffer_dir)
      begin
	yield buffer
      ensure
	buffer.close
      end
      @buffers_queue.push buffer
      buffer
    end

    def store_2ndmemory(ary)
      Log::info(self, "start store")
      open_2ndmemory do |io|
	while !ary.empty?
	  e = ary.shift
	  Marshal.dump(e, io)
	end
      end
      Log::info(self, "end store")
    end

    def restore_2ndmemory
      buf = @buffers_queue.pop
      io = buf.open
      queue = []
      begin
	loop do
	  queue.push Marshal.load(io)
	end
      rescue
      end
      buf.close!
      Log::info(self, "end restore")
      queue
    end
  end

#   class FileBufferdQueueObsolated
#     def initialize(policy)
#       @threshold = policy[:threshold]
#       @threshold ||= CONF.FILEBUFFEREDQUEUE_THRESHOLD

#       @push_queue = Queue.new
#       @pop_queue = @push_queue
#       @buffers_queue = nil

#       @queue_mutex = Mutex.new
#       @queue_cv = ConditionVariable.new
#     end

#     def push(e)
#       @queue_mutex.synchronize do
# 	@push_queue.push e
# 	@queue_cv.signal
# 	if @push_queue.size > @threshold
# 	  if @push_queue == @pop_queue
# 	    @push_queue = Queue.new
# 	  else
# 	    store_2ndmemory(@push_queue)
# 	    @push_queue = Queue.new
# 	  end
# 	end
#       end
#     end

#     def pop
#       @queue_mutex.synchronize do
# 	begin
# 	  e = @pop_queue.pop(true)
# 	rescue
# 	  if @pop_queue == @push_queue
# 	    @queue_cv.wait(@queue_mutex)
# 	  elsif @buffers_queue.nil?
# 	    @pop_queue = @push_queue
# 	  elsif @buffers_queue.empty?
# 	    @pop_queue = @push_queue
# 	    @push_queue = Queue.new
# 	    @buffers_queue = nil
# 	  else
# 	    @pop_queue = restore_2ndmeory
# 	  end
# 	  retry
# 	end
#       end
#     end

#     def init_2ndmemory
#       require "tempfile"

#       @buffer_dir = @policy[:buffer_dir]
#       @buffer_dir ||= CONF.TMP_DIR

#       @buffers_queue = Queue.new
#     end

#     def open_2ndmemeory(&block)
#       unless @buffers_queue
# 	init_2ndmemory
#       end
#       buffer = Tempfile.open("port-buffer-", @buffer_dir)
#       begin
# 	yield buffer
#       ensure
# 	buffer.close
#       end
#       @buffers_queue.push buffer
#       buffer
#     end

#     def store_2ndmemory(queue)
#       Log::info(self, "start store")
#       open_2ndmemory do |io|
# 	begin
# 	  loop do
# 	    e = queue.pop(false)
# 	    Marshal.dump(e, io)
# 	  end
# 	rescue
# 	end
#       end
#       Log::info(self, "end store")
#     end

#     def restore_2ndmemory
#       Log::info(self, "start restore")
#       buf = @buffers_queue.pop
#       io = buf.open
#       queue = Queue.new
#       begin
# 	loop do
# 	  queue.push Marshal.load(io)
# 	end
#       rescue
#       end
#       buf.close!
#       Log::info(self, "end restore")
#       queue
#     end
#   end
end
