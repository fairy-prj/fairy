# encoding: UTF-8

require "node/n-filter"
require "node/n-group-by"

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
	@key_value = {}
      end

      def push(key, value)
	@key_value[key] = [] unless @key_value.key?(key)
	@key_value[key].push value
      end

      def each(&block)
	@key_value.each &block
      end
    end

    class SimpleFileByKeyBuffer
      def initialize(policy)
	require "tempfile"

	@key_file = {}
	@buffer_dir = policy[:buffer_dir]
	@buffer_dir ||= CONF.TMP_DIR
      end

      def push(key, value)
	unless @key_file.key?(key)
	  @key_file[key] = Tempfile.open("mod-group-by-buffer-", @buffer_dir)
	end
	
	Marshal.dump(value, @key_file[key])
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
  end
end


