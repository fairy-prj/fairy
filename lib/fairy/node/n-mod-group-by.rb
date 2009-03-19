# encoding: UTF-8

require "fairy/node/n-filter"
require "fairy/node/n-group-by"

module Fairy
  class NModGroupBy<NGroupBy
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      
      @mod = CONF.N_MOD_GROUP_BY

      mod = CONF.HASH_MODULE
      require mod
      @hash_generator = Fairy::HValueGenerator.new(bjob.hash_seed)

    end

    def key(e)
      @hash_generator.value(super) % @mod
    end

    class NPostFilter<NSingleExportFilter
      Processor.def_export self

      def initialize(processor, bjob, opts, block_source)
	super
	@block_source = block_source

	@buffering_policy = @opts[:buffering_policy]
	@buffering_policy ||= CONF.MOD_GROUP_BY_BUFFERING_POLICY

      end

      def start
	super do
	  @key_value_buffer = 
	    eval("#{@buffering_policy[:buffering_class]}").new(@buffering_policy)
	  @hash_proc = BBlock.new(@block_source, @context, self)

	  @import.each do |e|
	    key = key(e)
	    @key_value_buffer.push(key, e)
	  end
	  @key_value_buffer.each do |key, values|
	    #Log::debug(self, key)
	    @export.push [key, values]
	  end
	end
      end

      def key(e)
	@hash_proc.yield(e)
      end
    end

    class OnMemoryBuffer
      def initialize(policy)
	@policy = policy

	@key_values = {}
	@key_values_mutex = Mutex.new
      end

      def push(key, value)
	@key_values_mutex.synchronize do
	  @key_values[key] = [] unless @key_values.key?(key)
	  @key_values[key].push value
	end
      end

      def each(&block)
	@key_values.each &block
      end
    end

    class SimpleFileByKeyBuffer
      def initialize(policy)
	require "tempfile"

	@key_file = {}
	@key_file_mutex = Mutex.new
	@buffer_dir = policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR
      end

      def push(key, value)
	@key_file_mutex.synchronize do
	  unless @key_file.key?(key)
	    @key_file[key] = Tempfile.open("mod-group-by-buffer-", @buffer_dir)
	  end
	
	  Marshal.dump(value, @key_file[key])
	end
      end

      def each(&block)
	@key_file.each do |key, file|
	  values = []
	  file.rewind
	  while !file.eof?
	    values.push Marshal.load(file)
	  end
#	  file.close
	  yield key, values
	end
      end
    end

    class SimpleCommandSortBuffer
      def initialize(policy)
	require "tempfile"

	@buffer_dir = policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR
	@buffer = Tempfile.open("mod-group-by-buffer-", @buffer_dir)
	@buffer_mutex = Mutex.new
      end

      def push(key, value)
	@buffer_mutex.synchronize do
	  @buffer << [Marshal.dump(key)].pack("m").tr("\n", ":")
	  @buffer << " "
	  @buffer << [Marshal.dump(value)].pack("m").tr("\n", ":")
	  @buffer << "\n"
	end
      end

      def each(&block)
	buffile = @buffer.path
	@buffer.close
	IO::popen("sort #{buffile}") do |io|
	  key = nil
	  values = []
	  io.each do |line|
	    
#Log::debug(self, line)

	    mk, mv = line.split(" ")
	    k = Marshal.load(mk.tr(":", "\n").unpack("m").first)
	    v = Marshal.load(mv.tr(":", "\n").unpack("m").first)
	    if key == k
	      values.push v
	    else
	      yield key, values
	      key = k
	      values = [v]
	    end
	  end
	end
      end
    end

    class CommandMergeSortBuffer<OnMemoryBuffer
      def initialize(policy)
	super

	@threshold = policy[:threshold]
	@threshold ||= CONF.MOD_GROUP_BY_CMSB_THRESHOLD

	@buffers = nil
      end

      def init_2ndmemory
	require "tempfile"

	@buffer_dir = @policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR

	@buffers = []
      end

      def open_buffer(&block)
	unless @buffers
	  init_2ndmemory
	end
	buffer = Tempfile.open("mod-group-by-buffer-", @buffer_dir)
	@buffers.push buffer
	if block_given?
	  begin
	    yield buffer
	  ensure
	    buffer.close
	  end
	else
	  buffer
	end
      end

      def push(key, value)
	super

	key_values = nil
	@key_values_mutex.synchronize do
	  if @key_values.size > @threshold
	    key_values = @key_values
	    @key_values = {}
	  end
	end
	if key_values
	  store_2ndmemory(key_values)
	end
      end

      def store_2ndmemory(key_values)
	Log::info(self, "start store")
	sorted = key_values.collect{|key, value| 
	  [[Marshal.dump(key)].pack("m").tr("\n", ":"), 
	    [Marshal.dump(value)].pack("m").tr("\n", ":")]}.sort_by{|e| e.first}

	open_buffer do |io|
	  sorted.each do |k, v|
	    io.puts "#{k}\t#{v}"
	  end
	end
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

	IO::popen("sort -m #{@buffers.collect{|b| b.path}.join(' ')}") do |io|
	  key = nil
	  values = []
	  io.each do |line|
	    mk, mv = line.split(/\s+/)
	    k = Marshal.load(mk.tr(":", "\n").unpack("m").first)
	    v = Marshal.load(mv.tr(":", "\n").unpack("m").first)
	    if key == k
	      values.push v
	    else
	      yield key, values
	      key = k
	      values = [v]
	    end
	  end
	end
      end
    end

    class MergeSortBuffer<CommandMergeSortBuffer

      def store_2ndmemory(key_values)
	Log::info(self, "start store")
	sorted = key_values.sort_by{|e| e.first}
	
	open_buffer do |io|
	  sorted.each do |key, value|
	    k = [Marshal.dump(key)].pack("m").tr("\n", ":")
	    v = [Marshal.dump(value)].pack("m").tr("\n", ":")
	    io.puts "#{k}\t#{v}"
	  end
	end
	Log::info(self, "end store")
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	end

	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))

	bufs = @buffers.collect{|buf|
	  buf.open
	  kv = read_line(buf)
	  [kv, buf]
	}.select{|kv, buf| !kv.nil?}.sort_by{|kv, buf| kv[0]}
	
	key = nil
	values = []
	while buf_min = bufs.shift
	  kv, buf = buf_min

	  if key == kv[0]
	    values.push kv[1]
	  else
	    yield key, values
	    key = kv[0]
	    values = [kv[1]]
	  end

	  next unless line = read_line(buf)
	  idx = bufs.rindex{|kv, b| kv[0] <= line[0]}
	  idx ? bufs.insert(idx+1, [line, buf]) : bufs.unshift([line, buf])
	end
	
      end

      def read_line(io)
	line = io.gets
	return line unless line
	mk, mv = line.split(/\s+/)
	k = Marshal.load(mk.tr(":", "\n").unpack("m").first)
	v = Marshal.load(mv.tr(":", "\n").unpack("m").first)
	[k, v]
      end
    end
  end
end


