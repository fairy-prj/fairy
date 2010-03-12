# encoding: UTF-8

require "fairy/node/n-filter"
require "fairy/node/n-group-by"

module Fairy
  class NModGroupBy<NGroupBy
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      
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

    class NPostFilter<NSingleExportFilter
      Processor.def_export self

      def initialize(processor, bjob, opts, block_source)
	super
	@block_source = block_source

	@buffering_policy = @opts[:buffering_policy]
	@buffering_policy ||= CONF.MOD_GROUP_BY_BUFFERING_POLICY

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
	  key = hash_key(e)
	  @key_value_buffer.push(key, e)
	  e = nil
	end

	@key_value_buffer.each do |key, values|
#Log::debug(self, values.inspect)
	  block.call [key, values]
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
    end

    class OnMemoryBuffer
      def initialize(njob, policy)
	@njob = njob
	@policy = policy

	@key_values = {}
	@key_values_mutex = Mutex.new

	@CHUNK_SIZE = CONF.MOD_GROUP_BY_CMSB_CHUNK_SIZE
      end

      def push(key, value)
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
          values = KeyValueStream.new(key, self)
          fiber = Fiber.new{yield key, values}
          vv.each do |v|
            values.concat v
            fiber.resume
          end
          values.push_eos
          fiber.resume
        end
        @key_values = nil
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

      def push(key, value)
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
      def initialize(njob, policy)
	require "fairy/share/fast-tempfile"

	@njob = njob
	@policy = policy

	@buffer_dir = policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR
	@buffer = FastTempfile.open("mod-group-by-buffer--#{@njob.no}", @buffer_dir)
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
	  unless values.empty?
	    yield key, values
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

      def push(key, value)
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
	sorted = key_values.collect{|key, vv| 
	  [[Marshal.dump(key)].pack("m").tr("\n", ":"),
           vv.collect{|values| [Marshal.dump(values)].pack("m").tr("\n", ":")}]}.sort_by{|e| e.first}

        open_buffer do |io|
	  sorted.each do |k, vv|
            vv.each do |v|
              io.puts "#{k}\t#{v}"
            end
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
	  values = []
          fiber = nil
	  io.each do |line|
	    mk, mv = line.split(/\s+/)
	    k = Marshal.load(mk.tr(":", "\n").unpack("m").first)
	    v = Marshal.load(mv.tr(":", "\n").unpack("m").first)
	    if key == k
	      values.concat v
              fiber.resume
	    else
              if fiber
                values.push_eos
                fiber.resume
              end
	      key = k
	      values = KeyValueStream.new(key, self)
              fiber = Fiber.new{yield key, values}
              values.concat v
              fiber.resume
	    end
	  end
          values.push_eos
          fiber.resume
	end
      end
    end

    class MergeSortBuffer<CommandMergeSortBuffer
      def store_2ndmemory(key_values)
#Log::debug(self, "start store")
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
#Log::debug(self, "end store")
      end

      def each_2ndmemory(&block)
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
	  @key_values = nil
	end
	Log::debug(self, @buffers.collect{|b| b.path}.join(" "))
	
        buffers = @buffers.collect{|buf|
          buf.open
          kv = read_line(buf.io)
          [kv, buf]
        }.select{|kv, buf| !kv.nil?}.sort_by{|kv, buf| kv[0]}
        @buffers = nil

        key = buffers.first.first.first
        values = KeyValueStream.new(key, self)
        fiber = Fiber.new{yield key, values}

        while buf_min = buffers.shift
          kv, buf = buf_min
          if key == kv[0]
            values.concat kv[1]
            fiber.resume
          else
            values.push_eos
            fiber.resume
            key = kv[0]
            values = KeyValueStream.new(key, self)
            fiber = Fiber.new{yield key, values}
            values.concat kv[1]
            fiber.resume
          end
          
          unless line = read_line(buf.io)
            buf.close!
            next
          end
          idx = buffers.rindex{|kv, b| kv[0] <= line[0]}
          idx ? buffers.insert(idx+1, [line, buf]) : buffers.unshift([line, buf])
        end
        values.push_eos
        fiber.resume
      end

      def read_line(io)
        begin
          k = Marshal.load(io)
          v = Marshal.load(io)
        rescue EOFError
          return nil
        end
        [k, v]
      end
    end

    class ExtMergeSortBuffer<MergeSortBuffer

      def each_2ndmemory(&block)
	require "deep-connect/deep-fork"
	
	unless @key_values.empty?
	  store_2ndmemory(@key_values)
          @key_values = nil
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
	  block.call key, values
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
	
        key = buffers.first.first.first
	values = KeyVauleStream.new(key, self)
        fiber = Fiber.new{yield key, values}
	while buf_min = bufs.shift
	  kv, buf = buf_min

	  if key == kv[0]
	    values.concat kv[1]
            fiber.resume
	  else
            values.push_eos
            fiber.resume
            key = kv[0]
            values = KeyValueStream.new(key, self)
            fiber = Fiber.new{yield key, values}
            values.concat kv[1]
            fiber.resume
	  end

	  next unless line = read_line(buf.io)
	  idx = bufs.rindex{|kv, b| kv[0] <= line[0]}
	  idx ? bufs.insert(idx+1, [line, buf]) : bufs.unshift([line, buf])
	end
        values.push_eos
        fiber.resume
	nil  # referenceが戻らないようにしている
      end
      DeepConnect.def_method_spec(self, "REF sub_each(){DVAL, DVAL}")

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
  end
end



