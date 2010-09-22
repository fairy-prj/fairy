# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
require "fairy/node/p-basic-group-by"

module Fairy
  class PGroupBy<PBasicGroupBy

    Processor.def_export self

    def initialize(id, ntask, bjob, opts, block_source)
      super

      @exports = []
       def @exports.each_pair(&block)
 	each_with_index{|item, idx| block.call(idx, item)}
       end

      @mod = opts[:n_mod_group_by] 
      @mod ||= CONF.N_MOD_GROUP_BY

      mod = opts[:hash_module]
      mod ||= CONF.HASH_MODULE
      require mod
      @hash_generator = Fairy::HValueGenerator.new(bjob.hash_seed)

      @hash_optimize = CONF.HASH_OPTIMIZE
      @hash_optimize = opts[:hash_optimize] if opts.key?(:hash_optimize)
    end

    def hash_key(e)
      @hash_generator.value(super) % @mod
    end

    class PPostFilter<PSingleExportFilter
      Processor.def_export self

      def initialize(id, ntask, bjob, opts, block_source)
	super
	@block_source = block_source

	@buffering_policy = @opts[:buffering_policy]
	@buffering_policy ||= CONF.MOD_GROUP_BY_BUFFERING_POLICY

	unless CONF.BUG234
	  @hash_optimize = CONF.HASH_OPTIMIZE
	  @hash_optimize = opts[:hash_optimize] if opts.key?(:hash_optimize)
	end
      end

#       def start
# 	super do
# 	  @key_value_buffer = 
# 	    eval("#{@buffering_policy[:buffering_class]}").new(@buffering_policy)
# 	  @hash_proc = BBlock.new(@block_source, @context, self)

# 	  @import.each do |e|
# 	    key = key(e)
# 	    @key_value_buffer.push(key, e)
# 	  end
# 	  @key_value_buffer.each do |key, values|
# 	    #Log::debug(self, key)
# 	    @export.push [key, values]
# 	  end
# 	  @key_value_buffer = nil
# 	end
#       end


      def basic_each_0(&block)
#	@key_value_buffer = 
#	  eval("#{@buffering_policy[:buffering_class]}").new(@buffering_policy)
	
	if @hash_optimize
	  @hash_proc = eval("proc{#{@block_source.source}}")
	else
	  @hash_proc = BBlock.new(@block_source, @context, self)
	end

	@input.group_by{|e| e}.each{|k, v|
	  block.call [k, v]
	}
      end

      def basic_each(&block)
	@key_value_buffer = 
	  eval("#{@buffering_policy[:buffering_class]}").new(self, @buffering_policy)
	if @hash_optimize
	  @hash_proc = eval("proc{#{@block_source.source}}")
	else
	  @hash_proc = BBlock.new(@block_source, @context, self)
	end

	@input.each do |e|
	  @key_value_buffer.push(e)
	  e = nil
	end
	@key_value_buffer.each do |kvs|
	  block.call kvs
	end
	@key_value_buffer = nil
      end

      def hash_key(e)
	@hash_proc.yield(e)
      end
    end

    class KeyValueStream
      include Enumerable

      EOS = :__KEY_VALUE_STREAM_EOS__

      def initialize(key, generator)
	@key = key
	@buf = []
      end

      attr_reader :key

      def push(e)
	@buf.push e
      end
      alias enq push

      def push_eos
	push EOS
      end

      def concat(elements)
	@buf.concat elements
      end
      
      def shift
	while @buf.empty?
	  Fiber.yield
	end
	@buf.shift
      end
      alias deq shift
      alias pop shift

      def each(&block)
	while (v = shift) != EOS
	  block.call v
	end
      end

      def size
	c = 0
	each{|v| c += 1}
	c
      end

#      def inspect
#	"#{self.class}<#{super}>"
#      end
    end

    class OnMemoryBuffer
      def initialize(njob, policy)
	@njob = njob
	@policy = policy

	@key_values = {}
	@key_values_mutex = Mutex.new

	@CHUNK_SIZE = CONF.MOD_GROUP_BY_CMSB_CHUNK_SIZE

	@log_id = format("%s[%s]", self.class.name.sub(/Fairy::/, ''), @njob.id)
      end

      attr_accessor :log_id

      def push(value)
	key = @njob.hash_key(value)

	@key_values_mutex.synchronize do
	  @key_values[key] = [[]] unless @key_values.key?(key)
	  if @CHUNK_SIZE < @key_values[key].last.size 
	    @key_values[key].push []
	  end
	  @key_values[key].last.push value
	end
      end
     
      def each(&block)
	@key_values.each do |key, vv|
	  kvs = KeyValueStream.new(key, nil)
	  vv.each{|v| kvs.concat v}
	  kvs.push_eos
	  block.call(kvs)
	end
      end
    end

    class SimpleFileByKeyBuffer
      def initialize(njob, policy)
	require "tempfile"

	@njob = njob
	@policy = policy

	@key_file = {}
	@key_file_mutex = Mutex.new
	@buffer_dir = policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR
      end

      def push(value)
	key = @njob.hash_key(value)

	@key_file_mutex.synchronize do
	  unless @key_file.key?(key)
	    @key_file[key] = Tempfile.open("mod-group-by-buffer-#{@njob.no}-", @buffer_dir)
	  end
	
	  # ruby BUG#2390の対応のため.
	  # Marshal.dump(value, @key_file[key])
	  Marshal.dump(value, @key_file[key].instance_eval{@tmpfile})
	end
      end

      def each(&block)
	@key_file.each do |key, file|
	  values = KeyValueStream.new(key, nil)
	  file.rewind
	  while !file.eof?
	    values.push Marshal.load(file)
	  end
	  values.push_eos
#	  file.close
	  
	  yield values
	end
      end
    end

    class SimpleCommandSortBuffer
      def initialize(njob, policy)
	require "fairy/share/fast-tempfile"

	@njob = njob
	@policy = policy

	@buffer_dir = policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR
	@buffer = FastTempfile.open("mod-group-by-buffer--#{@njob.no}", @buffer_dir)
	@buffer_mutex = Mutex.new
      end

      def push(value)
	key = @njob.hash_key(value)

	@buffer_mutex.synchronize do
	  @buffer.io << [Marshal.dump(key)].pack("m").tr("\n", ":")
	  @buffer.io << " "
	  @buffer.io << [Marshal.dump(value)].pack("m").tr("\n", ":")
	  @buffer.io << "\n"
	end
      end

      def each(&block)
	buffile = @buffer.path
	@buffer.close
	IO::popen("sort #{buffile}") do |io|
	  key = nil
	  values = nil
	  io.each do |line|
	    
#Log::debug(self, line)

	    mk, mv = line.split(" ")
	    k = Marshal.load(mk.tr(":", "\n").unpack("m").first)
	    v = Marshal.load(mv.tr(":", "\n").unpack("m").first)
	    if key == k
	      values.push v
	    else
	      if values
		values.push_eos
		yield values
	      end
	      values = KeyValueStream.new(k, self)
	      key = k
	      values.push v
	    end
	  end
	  if values
	    values.push_eos
	    yield values
	  end
	end
      end
    end

    class CommandMergeSortBuffer<OnMemoryBuffer
      def initialize(njob, policy)
	super

	@key_values_size = 0

	@threshold = policy[:threshold]
	@threshold ||= CONF.MOD_GROUP_BY_CMSB_THRESHOLD

	@buffers = nil
      end

      def init_2ndmemory
	require "fairy/share/fast-tempfile"

	@buffer_dir = @policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR

	@buffers = []
      end

      def open_buffer(&block)
	unless @buffers
	  init_2ndmemory
	end
	buffer = FastTempfile.open("mod-group-by-buffer-#{@njob.no}-", @buffer_dir)
	@buffers.push buffer
	if block_given?
	  begin
	    # ruby BUG#2390の対応のため.
	    # yield buffer
	    yield buffer.io
	  ensure
	    buffer.close
	  end
	else
	  buffer
	end
      end

      def push(value)
	super

	@key_values_size += 1
	key_values = nil
	@key_values_mutex.synchronize do
	  if @key_values_size > @threshold
	    key_values = @key_values
	    @key_values_size = 0
	    @key_values = {}
	  end
	  if key_values
	    store_2ndmemory(key_values)
	  end
	end
      end

      def store_2ndmemory(key_values)
	Log::info(self, "start store")
	sorted = key_values.collect{|key, values| 
	  [[Marshal.dump(key)].pack("m").tr("\n", ":"), 
	    [Marshal.dump(values)].pack("m").tr("\n", ":")]}.sort_by{|e| e.first}

	open_buffer do |io|
	  sorted.each do |k, v|
	    io.puts "#{k}\t#{v}"
	  end
	end
	sorted = nil
	Log::info(self, "end store")
      end

      def each(&block)
	if @buffers
	  each_2ndmemory &block
	else
	  super
	end
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	end

	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))

	IO::popen("sort -m -k1,1 #{@buffers.collect{|b| b.path}.join(' ')}") do |io|
	  key = nil
	  values = nil
	  io.each do |line|
	    mk, mv = line.split(/\s+/)
	    k = Marshal.load(mk.tr(":", "\n").unpack("m").first)
	    v = Marshal.load(mv.tr(":", "\n").unpack("m").first)
	    if key == k
	      values.concat v
	    else
	      if values
		values.push_eos
		yield values
	      end
	      key = k
	      values = KeyValueStream.new(key, self)
	      values.concat v
	    end
	  end
	  if values
	    values.push_eos
	    yield values
	  end
	end
      end
    end

    class MergeSortBuffer<CommandMergeSortBuffer
      class StSt
	def initialize(buffers)
	  @buffers = buffers.collect{|buf|
	    buf.open
	    kv = read_line(buf.io)
	    [kv, buf]
	  }.select{|kv, buf| !kv.nil?}.sort_by{|kv, buf| kv[0]}

	  @fiber = nil
	end

	def each(&block)
	  key = @buffers.first.first.first
	  values = KeyValueStream.new(key, self)
	  @fiber = Fiber.new{yield values}
	  while buf_min = @buffers.shift
	    kv, buf = buf_min
	    if key == kv[0]
	      values.concat kv[1]
	      @fiber.resume
	    else
	      values.push_eos
	      @fiber.resume
	      key = kv[0]
	      values = KeyValueStream.new(key, self)
	      @fiber = Fiber.new{yield values}
	      values.concat kv[1]
	      @fiber.resume
	    end
	    
	    unless line = read_line(buf.io)
	      buf.close!
	      next
	    end
	    idx = @buffers.rindex{|kv, b| kv[0] <= line[0]}
#	    idx ? @buffers.insert(idx+1, [line, buf]) : @buffers.unshift([line, buf])
	    buf_min[0] = line
	    idx ? @buffers.insert(idx+1, buf_min) : @buffers.unshift(buf_min)

	  end
	  values.push_eos
	  @fiber.resume
	end

	def read_line(io)
	  begin
	    k = Marshal.load(io)
	    v = Marshal.load(io)
	  rescue EOFError
	    return nil
	  rescue ArgumentError
	    Log::debug(self, "MARSHAL ERROR OCCURED!!")
	    io.seek(-1024, IO::SEEK_CUR)
	    buf = io.read(2048)
	    Log::debug(self, "File Contents: %s", buf)

	    raise
	  end
	  [k, v]
	end
      end

      def store_2ndmemory(key_values)
	Log::debug(self, "START STORE")
	sorted = key_values.sort_by{|e| e.first}
	
	open_buffer do |io|
	  sorted.each do |key, vv|
	    dk = Marshal.dump(key)
	    vv.each do |values|
	      io.write dk
	      Marshal.dump(values, io)
	    end

	  end
	end
	sorted = nil
	Log::debug(self, "FINISH STORE")
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::info(self, "Merge Start: #{@buffers.size} files")
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	stst = StSt.new(@buffers)
	@buffers = nil
	stst.each(&block)
      end
    end

    class ExtMergeSortBuffer<MergeSortBuffer

      def each_2ndmemory(&block)
	require "deep-connect/deep-fork"
	
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	end

	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))

	df = DeepConnect::DeepFork.fork(@njob.processor.deepconnect){|dc, ds|
	  $0 = "fairy processor sorter"

	  dc.export("Sorter", self)

	  finish_wait
#	  ds.close
#	  dc.stop
	  sleep 1
	}
	sorter = df.peer_deep_space.import("Sorter", true)
	sorter.sub_each {|key, values|
#	sorter.sub_each {|bigstr|
# 	  values = bigstr.split("\t").collect{|e| 
# 	    e.gsub(/(\\t|\\\\)/){|v| v == "\\t" ? "\t" : "\\"}
# 	  }
# 	  key = values.shift
	  block.call values
	  nil  # referenceが戻らないようにしている
	}
	sorter.finish
#	df.peer_deep_space.close
	@buffers.each{|buf| buf.close!}
	Process.waitpid(df.peer_pid)
      end

      def sub_each(&block)
	bufs = @buffers.collect{|buf|
	  buf.open
	  kv = read_line(buf.io)
	  [kv, buf]
	}.select{|kv, buf| !kv.nil?}.sort_by{|kv, buf| kv[0]}
	
	key = nil
	values = []
	while buf_min = bufs.shift
	  kv, buf = buf_min

	  if key == kv[0]
	    values.concat kv[1]
	  else
	    yield key, values unless values.empty?
	    key = kv[0]
	    values = kv[1]
	  end

	  next unless line = read_line(buf.io)
	  idx = bufs.rindex{|kv, b| kv[0] <= line[0]}
	  idx ? bufs.insert(idx+1, [line, buf]) : bufs.unshift([line, buf])
	end
	unless values.empty?
	  yield values
# 	  values.unshift key
# 	  bigstr = values.collect{|e| 
# 	    e.gsub(/[\\\t]/){|v| v == "\t" ? "\\t" : '\\\\'}
# 	  }.join("\t")
# 	  yield bigstr
	end
	nil  # referenceが戻らないようにしている
      end
#      DeepConnect.def_method_spec(self, "REF sub_each(){DVAL, DVAL}")

      def finish_wait
	@mx = Mutex.new
	@cv = ConditionVariable.new
	@mx.synchronize do
	  @cv.wait(@mx)
	end
      end

      def finish
	@cv.signal
      end

    end

    #
    # using: Depq(http://depq.rubyforge.org/)
    #
    class DepqMergeSortBuffer<MergeSortBuffer
      class StSt<MergeSortBuffer::StSt
	def initialize(buffers)
	  require "depq"

	  @buffers = Depq.new
	  buffers.each{|buf|
	    buf.open
	    kv = read_line(buf.io)
	    next unless kv
	    @buffers.insert [kv, buf], kv.first
	  }

	  @fiber = nil
	end

	def each(&block)
	  key = @buffers.find_min.first.first
	  values = KeyValueStream.new(key, self)
	  @fiber = Fiber.new{yield values}
	  while buf_min = @buffers.delete_min
	    kv, buf = buf_min
	    if key == kv[0]
	      values.concat kv[1]
	      @fiber.resume
	    else
	      values.push_eos
	      @fiber.resume
	      key = kv[0]
	      values = KeyValueStream.new(key, self)
	      @fiber = Fiber.new{yield values}
	      values.concat kv[1]
	      @fiber.resume
	    end
	    
	    unless line = read_line(buf.io)
	      buf.close!
	      next
	    end
	    @buffers.insert [line, buf], line[0]
	  end
	  values.push_eos
	  @fiber.resume
	end
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	stst = StSt.new(@buffers)
	@buffers = nil
	stst.each(&block)
      end
    end

    class DepqMergeSortBuffer2<DepqMergeSortBuffer
      class StSt<DepqMergeSortBuffer::StSt
	def each(&block)
	  key = @buffers.find_min.first.first
	  values = KeyValueStream.new(key, self)
	  @fiber = Fiber.new{yield values}
	  while buf_min = @buffers.find_min
	    kv, buf = buf_min
	    if key == kv[0]
	      values.concat kv[1]
	      @fiber.resume
	    else
	      values.push_eos
	      @fiber.resume
	      key = kv[0]
	      values = KeyValueStream.new(key, self)
	      @fiber = Fiber.new{yield values}
	      values.concat kv[1]
	      @fiber.resume
	    end
	    
	    unless line = read_line(buf.io)
	      buf.close!
	      @buffers.delete_min
	      next
	    end
#	    @buffers.replace_min [line, buf], line[0]
	    buf_min[0] = line
	    loc = @buffers.find_min_locator
	    loc.update_priority line[0]
	  end
	  values.push_eos
	  @fiber.resume
	end
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	stst = StSt.new(@buffers)
	@buffers = nil
	stst.each(&block)
      end
    end

    #
    # using: PriorityQueue(http://rubyforge.org/projects/priority-queue/)
    #
    class PQMergeSortBuffer<MergeSortBuffer
      class StSt<MergeSortBuffer::StSt
	class Pair
	  def initialize(kv, buf)
	    @key_values = kv
	    @buf = buf
	  end

	  attr_accessor :key_values
	  attr_accessor :buf

	  def key
	    @key_values.first
	  end

	  def values
	    @key_values.last
	  end
	end
	
	def initialize(buffers)
	  require "priority_queue"

	  @buffers = PriorityQueue.new
	  buffers.each{|buf|
	    buf.open
	    kv = read_line(buf.io)
	    next unless kv
	    @buffers.push Pair.new(kv, buf) , kv.first
	  }

	  @fiber = nil
	end

	def each(&block)
	  key = @buffers.min_key.key
	  values = KeyValueStream.new(key, self)
	  @fiber = Fiber.new{yield values}
	  while min_pair = @buffers.delete_min_return_key
#	    buf, kv = buf_min
	    if key == min_pair.key
	      values.concat min_pair.values
	      @fiber.resume
	    else
	      values.push_eos
	      @fiber.resume
	      key = min_pair.key
	      values = KeyValueStream.new(key, self)
	      @fiber = Fiber.new{yield values}
	      values.concat min_pair.values
	      @fiber.resume
	    end
	    
	    unless line = read_line(min_pair.buf.io)
	      min_pair.buf.close!
	      next
	    end
	    min_pair.key_values = line
	    @buffers.push min_pair, line[0]
	  end
	  values.push_eos
	  @fiber.resume
	end
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::info(self, "Merge Start: #{@buffers.size} files")
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	stst = StSt.new(@buffers)
	@buffers = nil
	stst.each(&block)
      end
    end

    class PQMergeSortBuffer2<MergeSortBuffer
      class StSt<MergeSortBuffer::StSt
	def initialize(buffers)
	  require "priority_queue"

	  @buffers = PriorityQueue.new
	  buffers.each{|buf|
	    buf.open
	    kv = read_line(buf.io)
	    next unless kv
	    @buffers.push [kv, buf], kv.first
	  }

	  @fiber = nil
	end

	def each(&block)
	  key = @buffers.min_key.first.first
	  values = KeyValueStream.new(key, self)
	  @fiber = Fiber.new{yield values}
	  while buf_min = @buffers.min_key
	    kv, buf = buf_min
	    if key == kv[0]
	      values.concat kv[1]
	      @fiber.resume
	    else
	      values.push_eos
	      @fiber.resume
	      key = kv[0]
	      values = KeyValueStream.new(key, self)
	      @fiber = Fiber.new{yield values}
	      values.concat kv[1]
	      @fiber.resume
	    end
	    
	    unless line = read_line(buf.io)
	      buf.close!
	      @buffers.delete_min
	      next
	    end
	    buf_min[0] = line
	    @buffers.change_priority buf_min, line[0]
	  end
	  values.push_eos
	  @fiber.resume
	end
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	stst = StSt.new(@buffers)
	@buffers = nil
	stst.each(&block)
      end
    end

    class DirectOnMemoryBuffer

      def initialize(njob, policy)
	@njob = njob
	@policy = policy

	@key_values = []
	@key_values_mutex = Mutex.new

	@CHUNK_SIZE = policy[:chunk_size]
	@CHUNK_SIZE ||= CONF.MOD_GROUP_BY_CMSB_CHUNK_SIZE

	@log_id = format("%s[%s]", self.class.name.sub(/Fairy::/, ''), @njob.id)
      end

      attr_accessor :log_id

      def push(value)
	@key_values_mutex.synchronize do
	  @key_values.push value
	end
      end
     
      def each(&block)
#	@key_values = @key_values.collect{|e| [@njob.hash_key(e), e]}.group_by{|k, e| k}.sort_by{|k, e| k}
	@key_values = @key_values.group_by{|e| @njob.hash_key(e)}.sort_by{|k, e| k}.collect{|k, values| kvs = KeyValueStream.new(k, nil); kvs.concat(values); kvs.push_eos; kvs}
	@key_values.each &block
      end
    end

    class DirectMergeSortBuffer<DirectOnMemoryBuffer
      def initialize(njob, policy)
	super

	@threshold = policy[:threshold]
	@threshold ||= CONF.MOD_GROUP_BY_CMSB_THRESHOLD

	@buffers = nil
      end

      def init_2ndmemory
	require "fairy/share/fast-tempfile"

	@buffer_dir = @policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR

	@buffers = []
      end

      def open_buffer(&block)
	unless @buffers
	  init_2ndmemory
	end
	buffer = FastTempfile.open("mod-group-by-buffer-#{@njob.no}-", @buffer_dir)
	@buffers.push buffer
	if block_given?
	  begin
	    # ruby BUG#2390の対応のため.
	    # yield buffer
	    yield buffer.io
	  ensure
	    buffer.close
	  end
	else
	  buffer
	end
      end

      def push(value)
	super

	key_values = nil
	@key_values_mutex.synchronize do
	  if @key_values.size > @threshold
	    key_values = @key_values
	    @key_values = []
	  end
	  if key_values
	    store_2ndmemory(key_values)
	  end
	end
      end

      def store_2ndmemory(key_values)
	Log::debug(self, "START STORE")
	key_values = key_values.sort_by{|e| @njob.hash_key(e)}
	
	open_buffer do |io|
	  key_values.each_slice(@CHUNK_SIZE) do |ary|
	    Marshal.dump(ary, io)
	  end
	end
	sorted = nil
	Log::debug(self, "FINISH STORE")
      end
      
      def each(&block)
	if @buffers
	  each_2ndmemory &block
	else
	  super
	end
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::info(self, "Merge Start: #{@buffers.size} files")
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	m = Merger.new(@njob, @buffers)
	m.each(&block)
      end

      class Merger
	def initialize(njob, buffers, cached_buffer_class = CachedBuffer)
	  @njob = njob
	  @buffers = buffers.collect{|buf| cached_buffer_class.new(@njob, buf)}.select{|buf| !buf.eof?}.sort_by{|buf| buf.key}

	  @key = nil
	end

	def each(&block)
	  while !@buffers.empty?
	    @key = @buffers.first.key
	    values = KeyValueStream.new(@key, self)
	    block.call @values
	  end
	end

	def each_by_key(&block)
	  while buf_min = @buffers.shift
	    vv_key = buf_min.key
	    unless  @key == vv_key
	      @buffers.unshift buf_min
	      return
	    end

	    buf_min.each_by_same_key(&block)

	    if buf_min.eof?
	      buf_min.close!
	      next
	    end
	    
	    if vv_key == buf_min.key
	      @buffers.unshift(buf_min)
	    else
	      idx = @buffers.rindex{|buf| buf.key <= buf_min.key}
	      idx ? @buffers.insert(idx+1, buf_min) : @buffers.unshift(buf_min)
	    end
	  end
	end

	def get_buf(values)
	  unless buf_min = @buffers.shift
	    values.push_eos
	    return
	  end

	  vv_key = buf_min.key
	  unless  @key == vv_key
	    values.push_eos
	    @buffers.unshift buf_min
	    return
	  end

	  vv = buf_min.shift_values
	  if vv
	    values.concat vv
	  end
	  if buf_min.eof?
	    buf_min.close!
	    return
	  end
	  
	  idx = @buffers.rindex{|buf| buf.key <= buf_min.key}
	  idx ? @buffers.insert(idx+1, buf_min) : @buffers.unshift(buf_min)
	end
      end

      class CachedBuffer
	extend Forwardable

	def initialize(njob, io)
	  @njob = njob
	  @io = io
	  io.open

	  @cache = []
	  @cache_pv = 0

	  @eof = false

	  read_buffer
	  @key = @njob.hash_key(@cache.first)
	end

	def_delegator :@io, :open
	def_delegator :@io, :close
	def_delegator :@io, :close!

	def eof?
	  @eof
	end

	def key
	  @key
	end

	def each_by_same_key(&block)
	  if @cache.size <= @cache_pv
	    read_buffer
	    return if @cache.empty?
	  end
	  
	  while @njob.hash_key(@cache[@cache_pv]) == @key
	    block.call @cache[@cache_pv]
	    @cache_pv += 1

	    if @cache.size <= @cache_pv
	      read_buffer
	      return if @cache.empty?
	    end
	  end
	  @key = @njob.hash_key(@cache[@cache_pv])
	end
	
	def shift_values
	  if @cache.empty?
	    read_buffer
	    return nil if @cache.empty?
	  end

	  idx = @cache.index{|v| @njob.hash_key(v) != @key}
	  if idx
	    vv = @cache.slice!(0, idx)
	    @key = @njob.hash_key(@cache.first)
	  else
	    vv = @cache
	    @cache = []
	  end
	  vv
	end

	def read_buffer
	  io = @io.io
	  begin
	    @cache = Marshal.load(io)
	  rescue EOFError
	    @eof = true
	    @cache = []
	  rescue ArgumentError
	    Log::debug(self, "MARSHAL ERROR OCCURED!!")
	    io.seek(-1024, IO::SEEK_CUR)
	    buf = io.read(2048)
	    Log::debug(self, "File Contents: %s", buf)
	    raise
	  end
#	  @key = @njob.hash_key(@cache.first)
	  @cache_pv = 0
	end
	
      end

      class KeyValueStream
	include Enumerable

	EOS = :__KEY_VALUE_STREAM_EOS__

	def initialize(key, merger)
	  @key = key
	  @merger = merger

	  @buf = []
	end
	attr_reader :key

	def push(e)
	  @buf.push e
	end
	alias enq push

	def push_eos
	  push EOS
	end

	def concat(elements)
	  @buf.concat elements
	end
	
	def shift
	  while @buf.empty?
	    @merger.get_buf(self)
	  end
	  @buf.shift
	end
	alias deq shift
	alias pop shift

	def each(&block)
	  @merger.each_by_key(&block)
	end

	def size
	  c = 0
	  each{|v| c += 1}
	  c
	end
      end
    end

    class DirectFBMergeSortBuffer<DirectMergeSortBuffer
      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::info(self, "Merge Start: #{@buffers.size} files")
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	m = Merger.new(@njob, @buffers)
	m.each(&block)
      end

      class Merger<DirectMergeSortBuffer::Merger
	def initialize(njob, buffers)
	  @njob = njob
	  @buffers = buffers.collect{|buf| CachedBuffer.new(@njob, buf)}.select{|buf| !buf.eof?}.sort_by{|buf| buf.key}

	  @key = nil
	end
      end

      class CachedBuffer<DirectMergeSortBuffer::CachedBuffer
	extend Forwardable

	def initialize(njob, io)
	  super
	  
	  @each_fb = Fiber.new{|block| each_sub(block)}
	end

# 	def key
# 	  if @cache.empty?
# 	    read_buffer
# 	  end
# 	  @key
# 	end

	def each_by_same_key(&block)
	  @each_fb.resume(block)
	end
	
	def each_sub(block)
	  if @cache.empty?
	    read_buffer
	    return if @cache.empty?
	  end

	  while !@cache.empty?
	    @cache.each do |e|
	      unless @njob.hash_key(e) == @key
		@key = @njob.hash_key(e)
		block = Fiber.yield
	      end
	      block.call e
	    end
	    read_buffer
	  end
	end

	def read_buffer
	  io = @io.io
	  begin
	    @cache = Marshal.load(io)
	  rescue EOFError
	    @eof = true
	    @cache = []
	  rescue ArgumentError
	    Log::debug(self, "MARSHAL ERROR OCCURED!!")
	    io.seek(-1024, IO::SEEK_CUR)
	    buf = io.read(2048)
	    Log::debug(self, "File Contents: %s", buf)
	    raise
	  end
#	  @key = @njob.hash_key(@cache.first)
	end
      end
    end

    class DirectPQMergeSortBuffer<DirectMergeSortBuffer
      
      def initialize(njob, policy)
	require "priority_queue"
	super
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::info(self, "Merge Start: #{@buffers.size} files")
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))

	m = Merger.new(@njob, @buffers)
	m.each(&block)
      end

      class Merger<DirectMergeSortBuffer::Merger

	def initialize(njob, buffers)
	  @njob = njob
	  @buffers = PriorityQueue.new
	  buffers.each{|buf|
	    cb = DirectMergeSortBuffer::CachedBuffer.new(@njob, buf)
	    next if cb.eof?
	    @buffers.push cb, cb.key
	  }

	  @key = nil
	end

	def each(&block)
	  while !@buffers.empty?
	    @key = @buffers.min_key.key
	    values = DirectMergeSortBuffer::KeyValueStream.new(@key, self)
	    block.call values
	  end
	end

	def each_by_key(&block)
	  while buf_min = @buffers.delete_min_return_key
	    vv_key = buf_min.key
	    unless  @key == vv_key
	      @buffers.push buf_min, buf_min.key
	      return
	    end

	    buf_min.each_by_same_key(&block)

	    if buf_min.eof?
	      buf_min.close!
	      return
	    end
	  
	    @buffers.push buf_min, buf_min.key
	  end
	end


	def get_buf(values)
	  unless buf_min = @buffers.delete_min_return_key
	    values.push_eos
	    return
	  end

	  vv_key = buf_min.key
	  unless  @key == vv_key
	    values.push_eos
	    @buffers.push buf_min, buf_min.key
	    return
	  end

	  vv = buf_min.shift_values
	  if vv
	    values.concat vv
	  end
	  if buf_min.eof?
	    buf_min.close!
	    return
	  end
	  
	  @buffers.push buf_min, buf_min.key
	end
      end
    end

    class DirectKBMergeSortBuffer<CommandMergeSortBuffer

      def store_2ndmemory(key_values)
	Log::debug(self, "START STORE")
	sorted = key_values.sort_by{|e| e.first}
	
	open_buffer do |io|
	  sorted.each do |key, vv|
	    vv.each do |values|
	      Marshal.dump(values, io)
	    end
	  end
	end
	sorted = nil
	Log::debug(self, "FINISH STORE")
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::info(self, "Merge Start: #{@buffers.size} files")
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	m = DirectMergeSortBuffer::Merger.new(@njob, @buffers, CachedBuffer)
	m.each(&block)
      end

      class CachedBuffer
	extend Forwardable

	def initialize(njob, io)
	  @njob = njob
	  @io = io
	  io.open

	  @cache = []

	  @eof = false

	  read_buffer
	  @key = @njob.hash_key(@cache.first)
	end

	def_delegator :@io, :open
	def_delegator :@io, :close
	def_delegator :@io, :close!

	def eof?
	  @eof
	end

	def key
	  @key
	end

	def each_by_same_key(&block)
	  loop do
	    @cache.each &block
	    read_buffer
	    return if @cache.empty?
	    unless @njob.hash_key(@cache.first) == @key
	      @key = @njob.hash_key(@cache.first)
	      return
	    end
	  end
	end
	
	def read_buffer
	  io = @io.io
	  begin
	    @cache = Marshal.load(io)
	  rescue EOFError
	    @eof = true
	    @cache = []
	  rescue ArgumentError
	    Log::debug(self, "MARSHAL ERROR OCCURED!!")
	    io.seek(-1024, IO::SEEK_CUR)
	    buf = io.read(2048)
	    Log::debug(self, "File Contents: %s", buf)
	    raise
	  end
	end
      end
    end

    class DirectKB2MergeSortBuffer<DirectKBMergeSortBuffer
      def store_2ndmemory(key_values)
	Log::debug(self, "START STORE")
	sorted = key_values.sort_by{|e| e.first}
	
	open_buffer do |io|
	  tmpary = []
	  tmpary_sz = 0
	  sorted.each do |key, vv|
	    vv.each do |values|
	      if tmpary_sz >= @CHUNK_SIZE
		Marshal.dump(tmpary, io)
		tmpary = []
		tmpary_sz = 0
	      end
	      tmpary.push values
	      tmpary_sz += values.size
	    end
	  end
	  if tmpary_sz > 0
	    Marshal.dump(tmpary, io)
	    tmpary = nil
	  end
	end
	sorted = nil
	Log::debug(self, "FINISH STORE")
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::info(self, "Merge Start: #{@buffers.size} files")
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
	m = DirectMergeSortBuffer::Merger.new(@njob, @buffers, CachedBuffer)
	m.each(&block)
      end

      class CachedBuffer
	extend Forwardable

	def initialize(njob, io)
	  @njob = njob
	  @io = io
	  io.open

	  @cache = []

	  @eof = false

	  read_buffer
	  @key = @njob.hash_key(@cache.first.first)
	end

	def_delegator :@io, :open
	def_delegator :@io, :close
	def_delegator :@io, :close!

	def eof?
	  @eof
	end

	def key
	  @key
	end

	def each_by_same_key(&block)
	  loop do
	    while vv = @cache.shift
	      unless @njob.hash_key(vv.first) == @key
		@cache.unshift vv
		@key = @njob.hash_key(vv.first)
		return
	      end
	      vv.each &block
	    end
	    read_buffer
	    return if @cache.empty?
	    unless @njob.hash_key(@cache.first.first) == @key
	      @key = @njob.hash_key(@cache.first.first)
	      return
	    end
	  end
	end
	
	def read_buffer
	  io = @io.io
	  begin
	    @cache = Marshal.load(io)
	  rescue EOFError
	    @eof = true
	    @cache = []
	  rescue ArgumentError
	    Log::debug(self, "MARSHAL ERROR OCCURED!!")
	    io.seek(-1024, IO::SEEK_CUR)
	    buf = io.read(2048)
	    Log::debug(self, "File Contents: %s", buf)
	    raise
	  end
	end
      end
    end
  end
end



