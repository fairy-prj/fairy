# encoding: UTF-8

require "forwardable"

require "fiber-mon"
require "fairy/share/fast-tempfile.rb"

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
#    DeepConnect::Reference
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

  class Import
    include Enumerable

    ::Fairy.add_port_keep_identity_class(self)

    END_OF_STREAM = :END_OF_STREAM

    class CTLTOKEN;end
    class CTLTOKEN_SET_NO_IMPORT<CTLTOKEN;end
    SET_NO_IMPORT = CTLTOKEN_SET_NO_IMPORT.new

    class CTLTOKEN_NULLVALUE;end
    TOKEN_NULLVALUE = CTLTOKEN_NULLVALUE.new

    class CTLTOKEN_DELAYED_ELEMENT<CTLTOKEN
      ::Fairy.add_port_keep_identity_class(self)

      def initialize(&b)
	@call_back = b
      end

      def get_element(import)
	@call_back.call(import)
      end
    end

    def initialize(policy = nil)

      @queuing_policy = policy
      @queuing_policy ||= CONF.PREQUEUING_POLICY

      case @queuing_policy
      when Hash
	@queue = eval("#{@queuing_policy[:queuing_class]}").new(@queuing_policy)
      else
	@queue = @queuing_policy
      end
      @log_import_ntimes_pop = CONF.LOG_IMPORT_NTIMES_POP
      @log_callback_proc = nil

      @no = nil
      @no_mutex = Mutex.new
      @no_cv = ConditionVariable.new

      @key = nil

      @no_import = nil

      @no_pop_elements = 0
      @no_eos = 0

      @DEBUG_PORT_WAIT = CONF.DEBUG_PORT_WAIT
    end

    def no
      @no_mutex.synchronize do
	while !@no
	  Log::debug(self, "Wait until set @no") if @DEBUG_PORT_WAIT
	  @no_cv.wait(@no_mutex)
	  Log::debug(self, "End: Wait until set @no") if @DEBUG_PORT_WAIT
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

    attr_accessor :key
    def add_key(key)
      @key = key
    end

    def no_import=(n)
      @no_import = n
      @queue.push SET_NO_IMPORT
    end

    def push(e)
      @queue.push e
      nil
    end
    DeepConnect.def_method_spec(self, "REF push(DVAL)")

    def push_buf(buf)
      if @queue.respond_to?(:push_all)
	@queue.push_all(buf)
	nil
      else
	begin 
	  buf.each{|e| @queue.push e}
	  nil
	rescue
	  Log::debug_exception(self)
	  raise
	end
      end
    end
    DeepConnect.def_method_spec(self, "REF push_buf(DVAL)")

    def push_strings(bigstr)
      strings = bigstr.split("\t").collect{|e| 
	e.gsub(/(\\t|\\\\)/){|v| v == "\\t" ? "\t" : "\\"}
      }

      if @queue.respond_to?(:push_all)
	@queue.push_all(strings)
	nil
      else
	begin 
	  strings.each{|e| @queue.push e}
	  nil
	rescue
	  Log::debug_exception(self)
	  raise
	end
      end
    end

    def push_keep_identity(e)
      @queue.push e
      nil
    end

    def pop
      while !@no_import or @no_import > @no_eos
	e = @queue.pop
	case e
	when CTLTOKEN_DELAYED_ELEMENT
	  e = e.get_element(self)
	end
	case e
	when CTLTOKEN_DELAYED_ELEMENT
	  e = e.get_element(import)
	when CTLTOKEN_SET_NO_IMPORT
	#when SET_NO_IMPORT
	  # do nothing
	when END_OF_STREAM
	  @no_eos += 1
	  Log::debug(self, "IMPORT EOS: #{@no_eos}/#{@no_import}")
	else
	  @no_pop_elements += 1
	  if @log_import_ntimes_pop && 
	      (@no_pop_elements % @log_import_ntimes_pop == 0 || 
	       @no_pop_elements == 1)
	    if @log_callback_proc
	      @log_callback_proc.call @no_pop_elements
	    else
	      Log::verbose(self, "IMPORT POP: #{@no_pop_elements}")
	    end
	  end

	  return e
	end
      end
      if @log_callback_proc
	@log_callback_proc.call "EOS"
      else
	Log::verbose(self, "IMPORT POP: EOS")
      end
      return nil
    end

    def each(&block)
      while !@no_import or @no_import > @no_eos
	e = @queue.pop
	case e
	when CTLTOKEN_DELAYED_ELEMENT
	  e = e.get_element(self)
	end

	case e
	when CTLTOKEN_SET_NO_IMPORT
	#when SET_NO_IMPORT
	  # do nothing
	when END_OF_STREAM
	  @no_eos += 1
	else
	  @no_pop_elements += 1
	  if @log_import_ntimes_pop && 
	      (@no_pop_elements % @log_import_ntimes_pop == 0 || 
	       @no_pop_elements == 1)
	    if @log_callback_proc
	      @log_callback_proc.call @no_pop_elements
	    else
	      Log::verbose(self, "IMPORT POP: #{@no_pop_elements}")
	    end
	  end
	  block.call(e)
	end
      end
      if @log_callback_proc
	@log_callback_proc.call "EOS"
      else
	Log::verbose(self, "IMPORT POP: EOS")
      end
    end

    def size
      size = 0
      each{size += 1}
      size
    end

    def context_eval(str)
      eval str
    end

    def set_log_callback(str = nil, bind = binding, file = __FILE__, line_no = __LINE__, &block)
      if str
        @log_callback_proc = eval(%{proc{#{str}}}, bind, file, line_no)
      else
        @log_callback_proc = block
      end
    end

    # 疑似対応
    def asynchronus_send_with_callback(method, *args, &call_back)
      ret = nil
      exp = nil
      begin
	ret = __send__(method, *args)
      rescue => exp
      end
      call_back.call(ret, exp)
    end

  end

  class Export
    END_OF_STREAM = :END_OF_STREAM

    ExportMonitor = FiberMon.new
    ExportMonitor.start

    def initialize(policy = nil)
      
      @queuing_policy = policy
      @queuing_policy ||= CONF.POSTQUEUING_POLICY
      @max_chunk = CONF.POSTQUEUE_MAX_TRANSFER_SIZE

#      @output_buf = []
#      @output_buf_mutex = Mutex.new
#      @output_buf_cv = ConditionVariable.new
      
      @output = nil
      @output_mutex = Mutex.new
      @output_cv = ConditionVariable.new

      @no = nil
      @no_mutex = Mutex.new
      @no_cv = ConditionVariable.new

      @key = nil

      @status = nil
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new

      @export_mon = ExportMonitor
      @pop_cv = @export_mon.new_cv
      @export_cv = @export_mon.new_cv
      
      case @queuing_policy
      when Hash
	klass = eval("#{@queuing_policy[:queuing_class]}")
	mon = @export_mon.new_mon
	cv = mon.new_cv
	@queue = klass.new(@queuing_policy, mon, cv)
      else
	@queue = @queuing_policy
      end

    end

    def no
      @no_mutex.synchronize do
	while !@no
	  Log::debug(self, "Wait until set @no.") if @DEBUG_PORT_WAIT
	  @no_cv.wait(@no_mutex)
	  Log::debug(self, "End: Wait until set @no")  if @DEBUG_PORT_WAIT
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

    attr_accessor :key
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
	  Log::debug(self, "Wait until set @output") if @DEBUG_PORT_WAIT
	  @output_cv.wait(@output_mutex)
	  Log::debug(self, "End: Wait until set @output") if @DEBUG_PORT_WAIT
	end
	@output
      end
    end

    def output=(output)
      @output_mutex.synchronize do
	@output = output
        @output_mq = output.deep_space.import_mq("MQ")
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
	  Log::debug(self, "@output is nil. Enter delay setting.") if @DEBUG_PORT_WAIT
	  output.no_import = n
	  Log::debug(self, "Exit delay setting.") if @DEBUG_PORT_WAIT
	end
	n
      end
    end

    def push(e)
      @queue.push e
      nil
    end

    def push_buf(buf)
      if @queue.respond_to?(:push_all)
	@queue.push_all(buf)
	nil
      else
	begin 
	  buf.each{|e| @queue.push e}
	  nil
	rescue
	  Log::debug_exception(self)
	  raise
	end
      end
    end

    def push_delayed_element(&block)
      @queue.push Import::CTLTOKEN_DELAYED_ELEMENT.new(&block)
    end

    def fib_pop
      @export_mon.synchronize do
	e = nil
	#@export_mon.entry{e = @queue.pop; @pop_cv.signal}
	Thread.start do
	  @export_mon.synchronize do
	    e = @queue.pop
	    @pop_cv.signal
	  end
	end
	@pop_cv.wait_until{e}
	e
      end
    end

    def fib_pop_all
      @export_mon.synchronize do
	e = nil
	#@export_mon.entry{e = @queue.pop; @pop_cv.signal}
	Thread.start do
	  @export_mon.synchronize do
	    e = @queue.pop_all
	    @pop_cv.signal
	  end
	end
	@pop_cv.wait_until{e}
	e
      end
    end


#     def push(e)
# #      @output_buf_mutex.synchronize do
# 	@output_buf.push e
# 	if @output_buf.size > 1000 || e == END_OF_STREAM
# 	  @output_buf_cv.signal
# 	end
# #      end
# #      @queue.push e
#     end

    def start_export0
      @export_mon.entry do
	if bug49 = CONF.DEBUG_BUG49
	  # BUG#49用
	  Log::debug(self, "export START")
	  mod = CONF.LOG_IMPORT_NTIMES_POP
	  n = 0
	end
	self.status = :EXPORT

	@export_mon.synchronize do
#	  while (e = fib_pop) != END_OF_STREAM
	  while (e = @queue.pop) != END_OF_STREAM
	    if bug49
	      # BUG#49用
	      n += 1
	      if (n % mod == 0 || n < 3)
		Log::debug(self, "EXPORT n: #{n}")
	      end
	    end
	    begin 
	      if PORT_KEEP_IDENTITY_CLASS_SET[e.class]
		@output_mq.push(@output, :push_keep_identity, e){
		  @export_mon.synchronize{@export_cv.broadcast}
                  nil
		}
	      else
		@output_mq.push(@output, :push, e) {
		  @export_mon.synchronize{@export_cv.broadcast}
                  nil
		}
	      end
	      @export_cv.wait
	    rescue DeepConnect::SessionServiceStopped
	      Log::debug_exception(self)
	      raise
	    rescue
	      Log::debug_exception(self)
	      raise
	    end
	    if bug49 && (n % mod == mod - 1 || n < 3)
	      Log::debug(self, "EXPORT e: #{n - mod + 1}")
	    end
	    @export_mon.yield
	  end
	end
	if bug49
	  # BUG#49用
	  Log::debug(self, "export PREFINISH0")
	end
	@output.push END_OF_STREAM
	if bug49
	  Log::debug(self, "export PREFINISH1")
	end
	self.status = END_OF_STREAM
	if bug49
	  Log::debug(self, "export FINISH")
	end
      end
      nil
    end

    def start_export
      unless @queue.respond_to?(:pop_all)
	return start_export0
      end
      
      @export_mon.entry do
	if bug49 = CONF.DEBUG_BUG49
	  # BUG#49用
	  Log::debug(self, "export START")
	  n = 0
	  mod = CONF.LOG_IMPORT_NTIMES_POP
	  limit = mod
	end
#	@export_mon.synchronize do
	  while (pops = @queue.pop_all).last != END_OF_STREAM
#	  while (pops = fib_pop_all).last != END_OF_STREAM
	    if bug49
	      n += pops.size
	      if n >= limit
		Log::debug(self, "EXPORT n: #{n}") 
		while limit > n
		  limit += mod
		end
	      end
	    end

	    begin 
	      export_elements(pops)
	    rescue DeepConnect::SessionServiceStopped
	      Log::debug_exception(self)
	      raise
	    rescue
	      Log::debug_exception(self)
	      raise
	    end
	    @export_mon.yield
	  end
	  export_elements(pops)
#	end

	if bug49
	  # BUG#49用
	  Log::debug(self, "export PREFINISH0")
	  #	@output.push END_OF_STREAM
	  Log::debug(self, "export PREFINISH1")
	end
	self.status = END_OF_STREAM
	if bug49
	  Log::debug(self, "export FINISH")
	end
      end
      nil
    end

#     def export_elements(elements)
#       max = CONF.POSTQUEUE_MAX_TRANSFER_SIZE

#       if elements.find{|e| PORT_KEEP_IDENTITY_CLASS_SET[e.class]}
# # 	elements.each do |e|
# #	  @output.push e
# #	end

# 	buf = []
#  	elements.each do |e|
#  	  if PORT_KEEP_IDENTITY_CLASS_SET[e.class]
# 	    start = 0
# 	    while buf.size > start
# 	      @output.push_buf buf[start, start+max]
# 	      start += max
# 	    end
#  	    @output.push e
#  	    buf = buf.clear
#  	  else
#  	    buf.push e
#  	  end
#  	end
# 	start = 0
# 	while buf.size > start
# 	  @output.push_buf buf[start, start+max]
# 	  start += max
# 	end
# 	buf.clear
#       else
# 	start = 0
# 	while elements.size > start
# 	  @output.push_buf elements[start, start+max]
# 	  start += max
# 	end
# 	elements.clear
#       end
#     end

    def export_elements(elements)
      start = 0
      string_p = nil
      elements.each_with_index do |e, idx|
#if false
	if e.class == String
	  string_p = true
	elsif string_p.nil?
	  string_p = false
	elsif string_p
	  exports_elements_sub_str(elements, start, idx-1)
	  start = idx
	  string_p = nil
	end
#end
	if PORT_KEEP_IDENTITY_CLASS_SET[e.class]
	  exports_elements_sub(elements, start, idx-1)
	  @export_mon.synchronize do
	    @output_mq.push(@output, :push_keep_identity, e){
	      @export_mon.synchronize do
		sended = true
		@export_cv.broadcast
	      end
              nil
	    }
	    @export_cv.wait_until{sended}
	  end
	  start = idx + 1
	end
      end
#      @output.push_buf elements
      if string_p
	exports_elements_sub_str(elements, start, elements.size-1)
      else
	exports_elements_sub(elements, start, elements.size-1)
      end
      elements.clear
    end

    def exports_elements_sub(elements, start, last, max = @max_chunk)
      while last >= start
	len = [max, last - start + 1].min
	@export_mon.synchronize do
	  sended = nil
	  @output_mq.push(@output, :push_buf, elements[start, len]){
	    @export_mon.synchronize do
	      sended = true
	      @export_cv.broadcast
	    end
            nil
	  }
	  @export_cv.wait_until{sended}
	end
	start += len
      end
    end

    def exports_elements_sub_str(elements, start, last, max = @max_chunk)
      while last >= start
	len = [max, last - start + 1].min
	bigstr = elements[start, len].collect{|e| 
	  e.gsub(/[\\\t]/){|v| v == "\t" ? "\\t" : '\\\\'}
	}.join("\t")
	@export_mon.synchronize do
	  sended = nil
	  @output_mq.push(@output, :push_strings, bigstr) {
	    @export_mon.synchronize do
	      sended = true
	      @export_cv.broadcast
	    end
            nil
	  }
	  @export_cv.wait_until{sended}
	end
	start += len
      end
    end


#     def start_export
#       Thread.start do
# 	self.status = :Export
# 	loop do
# 	  Buf = Nil
# 	  @Output_Buf_Mutex.Synchronize Do
# #P "Aaaaaaaaaaa:2"
# 	    @Output_Buf_Cv.Wait(@Output_Buf_Mutex)
# #P "Aaaaaaaaaaa:3"
# 	    Buf = @Output_Buf
# #P "Aaaaaaaaaaa:4"
# 	    @Output_Buf = []
# #P "Aaaaaaaaaaa:5"
# 	  End
# #P "Aaaaaaaaaaa:6"
# #Begin
# 	  @Output.Push_Buf Buf
# #Rescue
# #  P $@, $!
# #End
# #P "Aaaaaaaaaaa:7"
# 	  Break If Buf.Last == End_Of_Stream
# #P "Aaaaaaaaaaa:8"
# 	End
# #P "Aaaaaaaaaaa:9"

# # 	While (E = @Output_Buf.Pop) != End_Of_Stream
# # 	  Begin 
# # 	    If Port_Keep_Identity_Class_Set[E.Class]
# # 	      @Output.Push_Keep_Identity(E)
# # 	    Else
# # 	      Buf.Push E
# # 	      If Buf.Size > 1000
# # 		@Output.Push_Buf Buf.Join("")
# # 		Buf = []
# # 	      End
# # #	      @Output.Push E
# # 	    End
# # 	  Rescue Deepconnect::Sessionservicestopped
# # 	    Debug::Debug_Exception(Self)
# # 	    Raise
# # 	  End
# # 	End
# # 	If Buf.Size > 0
# # 	  @Output.Push_Buf Buf.Join("")
# # 	End
# 	@Output.Push End_Of_Stream
# 	Self.Status = End_Of_Stream
#       End
#       Nil
#     End

# ここから, 後で要検討
    def status=(val)
      @status_mutex.synchronize do
	@status_cv.broadcast{@status = val}
      end
    end

    def wait_finish(cv)
      @status_mutex.synchronize do
	while @status != END_OF_STREAM
	  @status_cv.wait(@status_mutex)
	end
#	@status = :EXPORT_FINISH
      end
    end

    def fib_wait_finish(cv)
      @status_cv = cv
      cv.wait_until{@status == END_OF_STREAM}
    end
# ここまで

  end

  module OnMemoryQueue
    def self.new(policy, queues_mon = nil, queues_cv = nil)
      if queues_mon
	raise "OnMemoryQueueはfiberをサポートしていません"
      end
      Queue.new
    end
  end

  class PoolQueue
    def initialize(policy, queue_mon = Monitor.new, queue_cv = queues_mon.new_cond)
      @policy = policy

      @queue_threshold = CONF.POOLQUEUE_POOL_THRESHOLD

      @queue = []
      @queue_mon = queue_mon
      @queue_cv = queue_cv
    end

    def push(e)
      @queue_mon.synchronize do
	@queue.push e
	if @queue.size >= @queue_threshold || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @queue_cv.broadcast
	end
      end
    end

    def push_all(buf)
      @queue_mon.synchronize do
	@queue.concat buf
	if @queue.size >= @queue_threshold || @queue.last == :END_OF_STREAM
	  @queue_cv.broadcast
	end
      end
    end

    def pop
      @queue_mon.synchronize do
	@queue_cv.wait_while{@queue.empty?}
	@queue.shift
      end
    end

    def pop_all
      @queue_mon.synchronize do
	@queue_cv.wait_while{@queue.size < @queue_threshold && @queue.last != :END_OF_STREAM}
#	buf = @queue.dup
#	@queue.clear
	buf, @queue = @queue, []
	buf
      end
    end
  end

  class SizedQueue
    extend Forwardable

    def initialize(policy, queues_mon = nil, queues_cv = nil)
      if queues_mon
	raise "OnMemoryQueueはfiberをサポートしていません"
      end

      size = policy[:size]
      size ||= CONF.ONMEMORY_SIZEDQUEUE_SIZE
      @queue = SizedQueue.new(size)
    end

    def_delegator :@queue, :push
    def_delegator :@queue, :pop
  end
  OnMemorySizedQueue = SizedQueue

  class SizedPoolQueue<PoolQueue
    def initialize(policy, queue_mon = Monitor.new, queue_cv = queues_mon.new_cond)
      super
      @max_size = policy[:size]
      @max_size ||= CONF.ONMEMORY_SIZEDQUEUE_SIZE

      @pop_cv = @queue_cv
      @push_cv = @queue_mon.new_cond
    end

    def push(e)
      @queue_mon.synchronize do
	@push_cv.wait_while{@queue.size > @max_size}
	@queue.push e
	if @queue.size >= @queue_threshold || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @pop_cv.broadcast
	end
      end
    end

    def push_all(buf)
      @queue_mon.synchronize do
	@push_cv.wait_while{@queue.size > @max_size}
	@queue.concat buf
	if @queue.size >= @queue_threshold || @queue.last == :END_OF_STREAM
	  @pop_cv.broadcast
	end
      end
    end

    def pop
      e = super
      @push_cv.broadcast
      e
    end

    def pop_all
      buf = super
      @push_cv.broadcast
      buf
    end
  end
  OnMemorySizedPoolQueue = SizedPoolQueue

  class ChunkedPoolQueue
    # multi push threads single pop thread
    def initialize(policy, queues_mon = Monitor.new, queues_cv = queues_mon.new_cond)
      @policy = policy

      @queue_threshold = CONF.POOLQUEUE_POOL_THRESHOLD
      @queue_max = CONF.POSTQUEUE_MAX_TRANSFER_SIZE

      @push_queue = []
      @push_queue_mutex = Mutex.new
      
      @queues = []
      @queues_mon = queues_mon
      @queues_cv = queues_cv

      @pop_queue = nil
#      @pop_queue_mutex = Mutex.new
    end

    attr_accessor :fib_cv

    def push(e)
      @push_queue_mutex.synchronize do
	@push_queue.push e
	if @push_queue.size >= @queue_threshold || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @queues_mon.synchronize do
	    @queues.push @push_queue 
	    @push_queue = []
	    @queues_cv.broadcast
	  end
	end
      end
    end

    def push_all(buf)
      @push_queue_mutex.synchronize do
	@push_queue.concat buf
	if @push_queue.size > @queue_threshold || 
	    @push_queue.last == :END_OF_STREAM
	  @queues_mon.synchronize do
	    @queues.push @push_queue
	    @push_queue = []
	    @queues_cv.broadcast
	  end
	end
      end
    end

    def pop
#      @pop_queue.synchronize do
      while !@pop_queue || @pop_queue.empty?
	@queues_mon.synchronize do
	  @queues_cv.wait_until{@pop_queue = @queues.shift}
	end
      end
      e = @pop_queue.shift
      @pop_queue = nil if @pop_queue.empty?
      e
    end

    def pop_all
#      @pop_queue.synchronize do
      while !@pop_queue
	@queues_mon.synchronize do
	  @queues_cv.wait_until{@pop_queue = @queues.shift}
	end
      end
      buf, @pop_queue = @pop_queue, nil
      buf
#      end
    end
  end

  class ChunkedSizedPoolQueue<ChunkedPoolQueue
    def initialize(policy, queues_mon = Monitor.new, queues_cv = queues_mon.new_cond)
      super
      @max_size = policy[:size]
      @max_size ||= CONF.ONMEMORY_SIZEDQUEUE_SIZE

      @queue_size = 0

      @pop_cv = @queues_cv
      @push_cv = @queues_mon.new_cond
    end

    def push(e)
      @queues_mon.synchronize do
	@push_cv.wait_while{@queue_size > @max_size}
	@queue_size += 1
      end
      super
    end

    def push_all(buf)
      @queues_mon.synchronize do
	@push_cv.wait_while{@queue_size > @max_size}
	@queue_size += buf.size
      end
      super
    end

    def pop
      e = super
      @queues_mon.synchronize do
	@queue_size -= 1
	@push_cv.broadcast if @queue_size <= @max_size
      end
      e
    end

    def pop_all
      buf = super
      @queues_mon.synchronize do
	@queue_size -= buf.size
	@push_cv.broadcast if @queue_size <= @max_size
      end
      buf
    end
  end

  class FileBufferdQueue
    def initialize(policy, queue_mon = Monitor.new, queue_cv = queue_mon.new_cond)

      @policy = policy
      @threshold = policy[:threshold]
      @threshold ||= CONF.FILEBUFFEREDQUEUE_THRESHOLD

      @push_queue = []
      @pop_queue = @push_queue
      @buffers_queue = nil

      @queue_mon = queue_mon
      @queue_cv = queue_cv
    end

    def push(e)
      @queue_mon.synchronize do
	@push_queue.push e
	@queue_cv.broadcast

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
      @queue_mon.synchronize do
	while @pop_queue.empty?
	  if @pop_queue.equal?(@push_queue)
	    @queue_cv.wait
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

    def pop_all
      @queue_mon.synchronize do
	while @pop_queue.empty?
	  if @pop_queue.equal?(@push_queue)
	    @queue_cv.wait
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
	pops = @pop_queue.dup
	@pop_queue.clear
	pops
      end
    end

    def init_2ndmemory
      @buffer_dir = @policy[:buffer_dir]
      @buffer_dir ||= CONF.TMP_DIR

      @buffers_queue = []
    end

    def open_2ndmemory(&block)
      unless @buffers_queue
	init_2ndmemory
      end
      buffer = FastTempfile.open("port-buffer-", @buffer_dir)
      begin
	yield buffer.io
      ensure
	buffer.close
      end
      @buffers_queue.push buffer
      buffer
    end

    def store_2ndmemory(ary)
#      Log::info(self, "start store")
      open_2ndmemory do |io|
	while !ary.empty?
	  e = ary.shift
	  Marshal.dump(e, io)
	end
      end
#      Log::info(self, "end store")
    end

    def restore_2ndmemory
      buf = @buffers_queue.shift
      io = buf.open
      queue = []
      begin
	loop do
	  queue.push Marshal.load(io)
	end
      rescue
      end
      buf.close!
#      Log::info(self, "end restore")
      queue
    end
  end

  class ChunkedFileBufferdQueue
    def initialize(policy, queue_mon = Monitor.new, queue_cv = queue_mon.new_cond)
      @policy = policy
      @threshold = policy[:threshold]
      @threshold ||= CONF.FILEBUFFEREDQUEUE_THRESHOLD

      @push_queue = []
      @push_queue_mutex = Mutex.new

      @buffers_queue = nil
      @buffers_queue_mon = queue_mon
      @buffers_queue_cv = queue_cv

      @pop_queue = nil
    end

    def push(e)
      @push_queue_mutex.synchronize do
	@push_queue.push e
	if @push_queue.size >= @threshold || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @buffers_queue_mon.synchronize do
	    if @pop_queue
	      store_2ndmemory(@push_queue)
	    else
	      @pop_queue = @push_queue
	    end
	    @push_queue = []
	    @buffers_queue_cv.broadcast
	  end
	end
      end
    end

    def push_all(buf)
      @push_queue_mutex.synchronize do
	@push_queue.concat buf
	if @push_queue.size > @threshold || 
	    @push_queue.last == :END_OF_STREAM
	  @buffers_queue_mon.synchronize do
	    if @pop_queue
	      store_2ndmemory(@push_queue)
	    else
	      @pop_queue = @push_queue
	    end
	    @push_queue = []
	    @buffers_queue_cv.broadcast
	  end
	end
      end
    end

    def pop
      while !@pop_queue || @pop_queue.empty?
	@buffers_queue_mon.synchronize do
	  if @buffers_queue
	    @pop_queue = restore_2ndmemory
	  else
	    @buffers_queue_cv.wait
	  end
	end
      end
      #e = @pop_queue.shift
      #@pop_queue = nil if @pop_queue.empty?
      #@e
      @pop_queue.shift
    end

    def pop_all
      while !@pop_queue || @pop_queue.empty?
	@buffers_queue_mon.synchronize do
	  if @buffers_queue
	    if @buffers_queue.empty?
	      @buffers_queue_cv.wait
	    else
	      @pop_queue = restore_2ndmemory
	    end
	  else
	    @buffers_queue_cv.wait
	  end
	end
      end
      #buf, @pop_queue = @pop_queue, nil
      #buf

      buf, @pop_queue = @pop_queue, []
      buf
    end

    def init_2ndmemory
      @buffer_dir = @policy[:buffer_dir]
      @buffer_dir ||= CONF.TMP_DIR

      @buffers_queue = []
    end

    def open_2ndmemory(&block)
      unless @buffers_queue
	init_2ndmemory
      end
      buffer = FastTempfile.open("port-buffer-", @buffer_dir)
      begin
	yield buffer.io
      ensure
	buffer.close
      end
      @buffers_queue.push buffer
      buffer
    end

    def store_2ndmemory(ary)
#      Log::debug(self, "start store")
      open_2ndmemory do |io|
	Marshal.dump(ary, io)
      end
#      Log::debug(self, "end store")
    end

    def restore_2ndmemory
#      Log::debug(self, "start restore")
      buf = @buffers_queue.shift
      io = buf.open
      queue = Marshal.load(io)
      buf.close!
#      Log::debug(self, "end restore")
      queue
    end
  end

  class SortedQueue
    def initialize(policy, queue_mon = Monitor.new, queue_cv = queue_mon.new_cond)
      @policy = policy

      @pool_threshold = policy[:pool_threshold]
      @pool_threshold ||= CONF.SORTEDQUEUE_POOL_THRESHOLD
      
      @threshold = policy[:threshold]
      @threshold ||= CONF.SORTEDQUEUE_THRESHOLD

      @push_queue = []
      @pop_queue = nil
      @buffers = nil

      @queue_mon = queue_mon
      @queue_cv = queue_cv

      @sort_by = policy[:sort_by]
      @sort_by ||= CONF.SORTEDQUEUE_SORTBY   

      if @sort_by.kind_of?(String)
	@sort_by = eval("proc{#{@sort_by}}")
      end
    end

    def push(e)
      @queue_mon.synchronize do
	@push_queue.push e
	if e == :END_OF_STREAM
	  @push_queue.pop
	  if @buffers
	    store_2ndmemory(@push_queue)
	    @push_queue = []
	    @pop_queue = []
	  else
	    begin
	      @pop_queue = @push_queue.sort_by{|e| @sort_by.call(e)}
	      @pop_queue.push :END_OF_STREAM
	    rescue
	      Log::debug_exception
	    end
	  end
	  @queue_cv.broadcast
	end
	if @push_queue.size >= @threshold
	  store_2ndmemory(@push_queue)
	  @push_queue = []
	end
      end
    end

    def pop
      @queue_mon.synchronize do
	@queue_cv.wait_while{@pop_queue.nil?}

	if @buffers.nil?
#Log::debug(self, @pop_queue.inspect)
	  return @pop_queue.shift
	else
	  pop_2ndmemory
	end
      end
    end

    def pop_all
      buf = []
      while e = pop
	buf.push e
	return buf if buf.size > @pool_threshold
      end
      buf
    end

#     def pop_all
#       @queue_mutex.synchronize do
# 	while @pop_queue.empty?
# 	  if @pop_queue.equal?(@push_queue)
# 	    @queue_cv.wait(@queue_mutex)
# 	  elsif @buffers_queue.nil?
# 	    @pop_queue = @push_queue
# 	  elsif @buffers_queue.empty?
# 	    @pop_queue = @push_queue
# 	    @push_queue = []
# 	    @buffers_queue = nil
# 	  else
# 	    @pop_queue = restore_2ndmemory
# 	  end
# 	end
# 	pops = @pop_queue.dup
# 	@pop_queue.clear
# 	pops
#       end
#     end


    def init_2ndmemory
      require "tempfile"

      @buffer_dir = @policy[:buffer_dir]
      @buffer_dir ||= CONF.TMP_DIR

      @buffers = []
      @merge_io = nil
    end

    def open_2ndmemory(&block)
      unless @buffers
	init_2ndmemory
      end
      buffer = Tempfile.open("port-buffer-", @buffer_dir)
      begin
	# ruby BUG#2390の対応のため.
#	yield buffer
	yield buffer.instance_eval{@tmpfile}
      ensure
	buffer.close
      end
      @buffers.push buffer
      buffer
    end

    def store_2ndmemory(ary)
      Log::debug(self, "start store: ")
      open_2ndmemory do |io|
	ary = ary.sort_by{|e| @sort_by.call(e)}
	while !ary.empty?
	  e = ary.shift
	  Marshal.dump(e, io)
	end
      end
      Log::debug(self, "end store")
    end

    def pop_2ndmemory
      unless @merge_io
	@buffers.each{|tf| tf.open}
	@merge_io = @buffers.map{|io| 
	  e = nil
	  begin
	    e = Marshal.load(io)
	  rescue EOFError
	    io.close!
	  end
	  [io, e]}.select{|io, v| !v.nil?}.sort_by{|io, v| @sort_by.call(v)}
      end
      unless io_min = @merge_io.shift
	return :END_OF_STREAM
      end
      
      io, min = io_min
      begin
	e = Marshal.load(io)
	@merge_io.push [io, e] 
	@merge_io = @merge_io.sort_by{|io, e| @sort_by.call(e)}
      rescue EOFError
	io.close!
      end
      min
    end
  end

  class OnMemorySortedQueue
    def initialize(policy, queue_mon = Monitor.new, queue_cv = queue_mon.new_cond)
      @policy = policy

      @pool_threshold = policy[:pool_threshold]
      @pool_threshold ||= CONF.SORTEDQUEUE_POOL_THRESHOLD

      @push_queue = []
      @pop_queue = nil

      @queue_mutex = queue_mon
      @queue_cv = queue_cv

      @sort_by = policy[:sort_by]
      @sort_by ||= CONF.SORTEDQUEUE_SORTBY   

      if @sort_by.kind_of?(String)
	@sort_by = eval("proc{#{@sort_by}}")
      end
    end

    def push(e)
      @queue_mon.synchronize do
	@push_queue.push e
	if e == :END_OF_STREAM
	  @push_queue.pop
	  push_on_eos
	end
      end
    end

    def push_on_eos
      begin
	@pop_queue = @push_queue.sort_by{|e| @sort_by.call(e)}
	@pop_queue.push :END_OF_STREAM
	@push_queue.clear
	@push_queue = nil
      rescue
	Log::debug_exception
      end
      @queue_cv.broadcast
    end

    def pop
      @queue_mon.synchronize do
	@queue_cv.wait_while{@pop_queue.nil?}
	@pop_queue.shift
      end
    end

    def pop_all
      @queue_mon.synchronize do
	@queue_cv.wait_while{@pop_queue.nil?}
	@pop_queue.shift(@pool_threshold)
      end
    end
  end

  class SortedQueue1<OnMemorySortedQueue
    def initialize(policy, queue_mon = Monitor.new, queue_cv = queue_mon.new_cond)
      super

      @threshold = policy[:threshold]
      @threshold ||= CONF.SORTEDQUEUE_THRESHOLD

      @buffers = nil
    end

    def push_on_eos
      if @push_queue.size <= @threshold
	super
      else
	store_2ndmemory(@push_queue)
	@push_queue.clear
	@push_queue = nil
	@queue_cv.broadcast
      end
    end

    def pop
      @queue_mon.synchronize do
	@queue_cv.wait_while{@pop_queue.nil? && @buffers.nil?}

	if @pop_queue.nil? && @buffers
	  @pop_queue = restore_2ndmemory
	end

	@pop_queue.shift
      end
    end

    def pop_all
      @queue_mon.synchronize do
	@queue_cv.wait_while{@pop_queue.nil? && @buffers.nil?}

	if @pop_queue.nil? || @pop_queue.empty?
	  @pop_queue = restore_2ndmemory
	end
	pops = @pop_queue
	@pop_queue = nil
	pops
      end
    end

    def init_2ndmemory
      require "tempfile"

      @buffer_dir = @policy[:buffer_dir]
      @buffer_dir ||= CONF.TMP_DIR

      @buffers = []
    end

    def open_2ndmemory(&block)
      unless @buffers
	init_2ndmemory
      end
      io = Tempfile.open("port-buffer-", @buffer_dir)
      @buffers.push io
      begin
	# ruby BUG#2390の対応のため.
#	yield io
	yield io.instance_eval{@tmpfile}
      ensure
	io.close
      end
      @buffers
    end

    def store_2ndmemory(ary)
      Log::debug(self, "start store: ")
      ary = ary.sort_by{|e| @sort_by.call(e)}
      
      while !ary.empty?
	open_2ndmemory do |io|
	  buf = ary.shift(@pool_threshold)  
	  Marshal.dump(buf, io)
	end
      end
      Log::debug(self, "end store")
    end

    def restore_2ndmemory
      io = @buffers.shift
      io.open
      buf = Marshal.load(io)
      if @buffers.empty?
	buf.push :END_OF_STREAM 
      end
      io.close!
      buf
    end
  end
end
