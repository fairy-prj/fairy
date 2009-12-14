# encoding: UTF-8

require "forwardable"

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

  DEBUG_PORT_WAIT = CONF.DEBUG_PORT_WAIT

  class Import
    include Enumerable

    ::Fairy.add_port_keep_identity_class(self)

    END_OF_STREAM = :END_OF_STREAM

    class CTLTOKEN;end
    class CTLTOKEN_SET_NO_IMPORT<CTLTOKEN;end
    SET_NO_IMPORT = CTLTOKEN_SET_NO_IMPORT.new

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
  end

  class Export
    END_OF_STREAM = :END_OF_STREAM

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
      Thread.start do
	if bug49 = CONF.DEBUG_BUG49
	  # BUG#49用
	  Log::debug(self, "export START")
	  mod = CONF.LOG_IMPORT_NTIMES_POP
	  n = 0
	end
	self.status = :EXPORT
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
	      @output.push_keep_identity(e)
	    else
	      @output.push e
	    end
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
      
      Thread.start do
	if bug49 = CONF.DEBUG_BUG49
	  # BUG#49用
	  Log::debug(self, "export START")
	  n = 0
	  mod = CONF.LOG_IMPORT_NTIMES_POP
	  limit = mod
	end
	while (pops = @queue.pop_all).last != END_OF_STREAM
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
	end
	export_elements(pops)

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
      elements.each_with_index do |e, idx|
	if PORT_KEEP_IDENTITY_CLASS_SET[e.class]
	  exports_elements_sub(elements, start, idx-1)
	  @output.push_keep_identity e
	  start = idx + 1
	end
      end
#      @output.push_buf elements
      exports_elements_sub(elements, start, elements.size-1)
      elements.clear
    end

    def exports_elements_sub(elements, start, last, max = @max_chunk)
      while last >= start
	len = [max, last - start + 1].min
	@output.push_buf elements[start, len]
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
#	@status = :EXPORT_FINISH
      end
    end
  end

  module OnMemoryQueue
    def self.new(policy)
      Queue.new
    end
  end

  class PoolQueue
    def initialize(policy)
      @policy = policy

      @queue_threshold = CONF.POOLQUEUE_POOL_THRESHOLD

      @queue = []
      @queue_mutex = Mutex.new
      @queue_cv = ConditionVariable.new
    end

    def push(e)
      @queue_mutex.synchronize do
	@queue.push e
#Log::debug(self) do |io|
#	  if e == :END_OF_STREAM
#	    io.print e
#      end
#      end
	    
	if @queue.size >= @queue_threshold || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @queue_cv.signal
	end
      end
    end

    def push_all(buf)
      @queue_mutex.synchronize do
	@queue.concat buf
	if @queue.size >= @queue_threshold || @queue.last == :END_OF_STREAM
	  @queue_cv.signal
	end
      end
    end

    def pop
      @queue_mutex.synchronize do
	while @queue.empty?
	  @queue_cv.wait(@queue_mutex)
	end
	@queue.shift
      end
    end

    def pop_all
      @queue_mutex.synchronize do
	while @queue.size < @queue_threshold && @queue.last != :END_OF_STREAM
	  @queue_cv.wait(@queue_mutex)
	end
#	buf = @queue.dup
#	@queue.clear
	buf, @queue = @queue, []
	buf
      end
    end
  end

  class OnMemorySizedQueue
    extend Forwardable

    def initialize(policy)
      size = policy[:size]
      size ||= CONF.ONMEMORY_SIZEDQUEUE_SIZE
      @queue = SizedQueue.new(size)
    end

    def_delegator :@queue, :push
    def_delegator :@queue, :pop
  end

  class OnMemorySizedPoolQueue<PoolQueue
    def initialize(policy)
      super
      @max_size = policy[:size]
      @max_size ||= CONF.ONMEMORY_SIZEDQUEUE_SIZE

      @pop_cv = @queue_cv
      @push_cv = ConditionVariable.new
    end

    def push(e)
      @queue_mutex.synchronize do
	while @queue.size > @max_size
	  @push_cv.wait(@queue_mutex)
	end
	@queue.push e
	if @queue.size >= @queue_threshold || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @pop_cv.signal
	end
      end
    end

    def push_all(buf)
      @queue_mutex.synchronize do
	while @queue.size > @max_size
	  @push_cv.wait(@queue_mutex)
	end
	@queue.concat buf
	if @queue.size >= @queue_threshold || @queue.last == :END_OF_STREAM
	  @pop_cv.signal
	end
      end
    end

    def pop
      e = super
      @push_cv.signal
      e
    end

    def pop_all
      buf = super
      @push_cv.signal
      buf
    end
  end

  class ChunkedPoolQueue
    # multi push threads single pop thread
    def initialize(policy)
      @policy = policy

      @queue_threshold = CONF.POOLQUEUE_POOL_THRESHOLD
      @queue_max = CONF.POSTQUEUE_MAX_TRANSFER_SIZE


      @push_queue = []
      @push_queue_mutex = Mutex.new
      
      @queues = []
      @queues_mutex = Mutex.new
      @queues_cv = ConditionVariable.new

      @pop_queue = nil
#      @pop_queue_mutex = Mutex.new
    end

    def push(e)
      @push_queue_mutex.synchronize do
	@push_queue.push e
	if @push_queue.size >= @queue_threshold || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @queues_mutex.synchronize do
	    @queues.push @push_queue 
	    @push_queue = []
	    @queues_cv.signal
	  end
	end
      end
    end

    def push_all(buf)
      @push_queue_mutex.synchronize do
	@push_queue.concat buf
	if @push_queue.size > @queue_threshold || 
	    @push_queue.last == :END_OF_STREAM
	  @queues_mutex.synchronize do
	    @queues.push @push_queue
	    @push_queue = []
	    @queues_cv.signal
	  end
	end
      end
    end

    def pop
#      @pop_queue.synchronize do
      while !@pop_queue || @pop_queue.empty?
	@queues_mutex.synchronize do
	  while !(@pop_queue = @queues.shift)
	    @queues_cv.wait(@queues_mutex)
	  end
	end
      end
      @pop_queue.shift
    end

    def pop_all
#      @pop_queue.synchronize do
	while !@pop_queue
	  @queues_mutex.synchronize do
	    while !(@pop_queue = @queues.shift)
	      @queues_cv.wait(@queues_mutex)
	    end
	  end
	end
	buf, @pop_queue = @pop_queue, nil
	buf
#      end
    end
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

    def pop_all
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
	pops = @pop_queue.dup
	@pop_queue.clear
	pops
      end
    end

    def init_2ndmemory
      @buffer_dir = @policy[:buffer_dir]
      @buffer_dir ||= CONF.TMP_DIR

      @buffers_queue = Queue.new
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

  class SortedQueue
    def initialize(policy)
      @policy = policy

      @pool_threshold = policy[:pool_threshold]
      @pool_threshold ||= CONF.SORTEDQUEUE_POOL_THRESHOLD
      
      @threshold = policy[:threshold]
      @threshold ||= CONF.SORTEDQUEUE_THRESHOLD

      @push_queue = []
      @pop_queue = nil
      @buffers = nil

      @queue_mutex = Mutex.new
      @queue_cv = ConditionVariable.new

      @sort_by = policy[:sort_by]
      @sort_by ||= CONF.SORTEDQUEUE_SORTBY   

      if @sort_by.kind_of?(String)
	@sort_by = eval("proc{#{@sort_by}}")
      end

    end

    def push(e)
      @queue_mutex.synchronize do
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
	  @queue_cv.signal 
	end
	if @push_queue.size >= @threshold
	  store_2ndmemory(@push_queue)
	  @push_queue = []
	end
      end
    end

    def pop
      @queue_mutex.synchronize do
	while @pop_queue.nil?
	  @queue_cv.wait(@queue_mutex)
	end

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
    def initialize(policy)
      @policy = policy

      @pool_threshold = policy[:pool_threshold]
      @pool_threshold ||= CONF.SORTEDQUEUE_POOL_THRESHOLD

      @push_queue = []
      @pop_queue = nil

      @queue_mutex = Mutex.new
      @queue_cv = ConditionVariable.new

      @sort_by = policy[:sort_by]
      @sort_by ||= CONF.SORTEDQUEUE_SORTBY   

      if @sort_by.kind_of?(String)
	@sort_by = eval("proc{#{@sort_by}}")
      end
    end

    def push(e)
      @queue_mutex.synchronize do
	@push_queue.push e
	if e == :END_OF_STREAM
	  push_on_eos
	end
      end
    end

    def push_on_eos
      @push_queue.pop
      begin
	@pop_queue = @push_queue.sort_by{|e| @sort_by.call(e)}
	@pop_queue.push :END_OF_STREAM
	@push_queue.clear
	@push_queue = nil
      rescue
	Log::debug_exception
      end
      @queue_cv.signal 
    end

    def pop
      @queue_mutex.synchronize do
	while @pop_queue.nil?
	  @queue_cv.wait(@queue_mutex)
	end

	@pop_queue.shift
      end
    end

    def pop_all
      @queue_mutex.synchronize do
	while @pop_queue.nil?
	  @queue_cv.wait(@queue_mutex)
	end
	@pop_queue.shift(@pool_threshold)
      end
    end
  end

  class SortedQueue1<OnMemorySortedQueue
    def initialize(policy)
      super

      @threshold = policy[:threshold]
      @threshold ||= CONF.SORTEDQUEUE_THRESHOLD

      @buffer = nil
    end

    def push_on_eos
      if @push_queue.size <= @threshold
	super
      else
	store_2ndmemory(@push_queue)
	@push_queue.clear
	@push_queue = nil
	@queue_cv.signal 
      end
    end


    def pop
      @queue_mutex.synchronize do
	while @pop_queue.nil? && @buffer.nil?
	  @queue_cv.wait(@queue_mutex)
	end

	if @pop_queue.nil? && @buffer
	  @pop_queue = pop_all_2ndmemory
	end

	@pop_queue.shift
      end
    end

    def pop_all
      @queue_mutex.synchronize do
	while @pop_queue.nil? && buffer.nil?
	  @queue_cv.wait(@queue_mutex)
	end

	if @pop_queue.nil? && @buffer
	  @pop_queue = pop_all_2ndmemory
	end
	@pop_queue.shift(@pool_threshold)
      end
    end

    def init_2ndmemory
      require "tempfile"

      @buffer_dir = @policy[:buffer_dir]
      @buffer_dir ||= CONF.TMP_DIR

      @buffers = []
      @merge_io = nil
    end

    def open_2ndmemory(&block)
      unless @buffer
	init_2ndmemory
      end
      @buffer = Tempfile.open("port-buffer-", @buffer_dir)
      begin
	# ruby BUG#2390の対応のため.
#	yield buffer
	yield buffer.instance_eval{@tmpfile}
      ensure
	@buffer.close
      end
      @buffer
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

    def pop_all_2ndmemory
      @buffer.open

      buf = []
      begin
	while e = Marshal.load(@buffer)
	  buf.push e
	end
      rescue EOFError
	@buffer.close!
      end
      buf
    end
  end
end
