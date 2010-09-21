# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
require "fairy/node/p-basic-group-by"

module Fairy
  module PSort
    class PPreSort<PBasicGroupBy
      Processor.def_export self

      ST_ALL_IMPORTED = :ST_ALL_IMPORTED
      ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
      ST_EXPORT_FINISH = :ST_EXPORT_FINISH

      def initialize(id, ntask, bjob, opts, block_source)
	super
	@block_source = block_source

	@exports = []
	def @exports.each_pair(&block)
	  each_with_index{|item, idx| block.call(idx, item)}
	end
	@exports_queue = Queue.new
	
	@counter = []

	#start_watch_exports
      end

      def add_export(key, export)
	@exports[key] = export
	#      @exports_queue.push [key, export]
	# [BUG#171]同期処理でないとまずい.
	@bjob.add_exports(key, export, self)
      end

      def start_export
	Log::debug(self, "START_EXPORT")

	start do
	  sample_line_no = @opts[:sampling_max]
	  sample_line_no ||= CONF.SORT_SAMPLING_MAX

	  hash_opt = @opts[:hash_optimize]
	  hash_opt = CONF.HASH_OPTIMIZE if hash_opt.nil?
	  
	  if hash_opt
	    @key_proc = eval("proc{#{@block_source.source}}", @context.binding)
	  else
	    @key_proc = BBlock.new(@block_source, @context, self)
	  end
	  
	  buf = []
	  no = 0
	  begin
	    sampling = true
	    @input.each do |e|
	      if sampling
		no += 1
		buf.push e
		if no >= sample_line_no
		  sampling = false
		  pile_sample(buf)
		  buf.each{|e| hashing(e)}
		end
	      else
		hashing(e)
	      end
	    end
	    if sampling
	      pile_sample(buf)
	      buf.each{|e| hashing(e)}
	    end
	  rescue
	    Log::debug_exception(self)
	    raise
	  ensure
	    @exports_queue.push nil
	    @exports.each_pair{|key, export| 
	      Log::debug(self, "G0 #{key} => #{@counter[key]}")	    
	      export.push END_OF_STREAM}
	  end
	end
      end

      def pile_sample(buf)
	policy = @opts[:postqueuing_policy]

	@pvs = @bjob.get_pvs(buf)

Log::debug(self, "%s", @pvs.inspect)

	(@pvs.size+1).times do |idx|
	  export = Export.new(policy)
	  @exports[idx] = export
	  
	  export.njob_id = @id
	  export.no = idx
	  export.add_key(idx)
	  add_export(idx, export)
	  @counter[idx] = 0
	end
      end

      def hashing(e)
	unless idx = @pvs.find_index{|pv| @key_proc.call(e) <= @key_proc.call(pv)}
	  idx = @pvs.size
	end

#Log::debug(self, "#{@pvs.inspect}")
#Log::debug(self, "#{idx}")
	

	export = @exports[idx]
	export.push e
	@counter[idx] += 1
      end

      def hash_key(e)
	@key_proc.yield(e)
      end
    end

    class PPostSort<PSingleExportFilter
      Processor.def_export self

      def initialize(id, ntask, bjob, opts, block_source)
	super
	@block_source = block_source

	@buffering_policy = @opts[:buffering_policy]
	@buffering_policy ||= CONF.SORT_BUFFERING_POLICY

	unless CONF.BUG234
	  @hash_optimize = CONF.HASH_OPTIMIZE
	  @hash_optimize = opts[:hash_optimize] if opts.key?(:hash_optimize)
	end
      end

      def basic_each(&block)
	@key_value_buffer = 
	  eval("#{@buffering_policy[:buffering_class]}").new(self, @buffering_policy)
	if @hash_optimize
	  @hash_proc = eval("proc{#{@block_source.source}}")
	else
	  @hash_proc = BBlock.new(@block_source, @context, self)
	end

	case @key_value_buffer
	when PGroupBy::DirectOnMemoryBuffer

	  @input.each do |e|
	    @key_value_buffer.push(e)
	    e = nil
	  end
	  @key_value_buffer.each do |key, values|
	    values.each(&block)
	  end
	  @key_value_buffer = nil
	  
	else
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
      end

      def hash_key(e)
	@hash_proc.yield(e)
      end
    end


  end
end


