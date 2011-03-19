# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"

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
	  each_with_index do |item, idx| 
	    block.call(idx, item) if item
	  end
	end
	@exports_queue = XThread::Queue.new
	
	@counter = []

	@pvs = nil
	if @opts[:pvs]
	  @pvs = @opts[:pvs].dc_deep_copy
	end

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

	  hash_opt = @opts[:cmp_optimize]
	  hash_opt = CONF.SORT_CMP_OPTIMIZE if hash_opt.nil?
	  
	  if hash_opt
	    @key_proc = eval("proc{#{@block_source.source}}", @context.binding)
	  else
	    @key_proc = BBlock.new(@block_source, @context, self)
	  end
	  
	  buf = []
	  no = 0
	  begin
	    if @pvs
	      sampling = false
Log::debugf(self, "%s", @pvs.inspect)
	      init_exports
	    elsif self.no == 0
	      sampling = true
	    else
	      sampling = false
	      @pvs = @bjob.get_pvs
Log::debugf(self, "%s", @pvs.inspect)
	      init_exports
	    end
	      
	    @input.each do |e|
	      if sampling
		no += 1
		buf.push e
		if no >= sample_line_no
		  sampling = false
		  @pvs = @bjob.get_pvs(buf)
Log::debugf(self, "%s", @pvs.inspect)
		  init_exports
		  buf.each{|e| hashing(e)}
		end
	      else
		hashing(e)
	      end
	    end
	    if sampling
	      @pvs = @bjob.get_pvs(buf)
Log::debugf(self, "%s", @pvs.inspect)
	      init_exports
	      buf.each{|e| hashing(e)}
	    end
	  rescue
	    Log::debug_exception(self)
	    raise
	  ensure
	    @exports_queue.push nil
	    @exports.each_pair do |key, export| 
	      next unless export
	      Log::debug(self, "G0 #{key} => #{@counter[key]}")	    
	      export.push END_OF_STREAM
	    end
	  end
	end
      end

      def init_exports
	policy = @opts[:postqueuing_policy]
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
	if Import::CTLTOKEN_NULLVALUE === (key = @key_proc.call(e))
	  return
	end
	
	unless idx = @pvs.find_index{|pv| key < @key_proc.call(pv)}
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
	  @cmp_optimize = CONF.SORT_CMP_OPTIMIZE
	  @cmp_optimize = opts[:cmp_optimize] if opts.key?(:cmp_optimize)
	end
      end

      def basic_each(&block)
	@key_value_buffer = 
	  eval("#{@buffering_policy[:buffering_class]}").new(self, @buffering_policy)
	if @cmp_optimize
	  @hash_proc = eval("proc{#{@block_source.source}}")
	else
	  @hash_proc = BBlock.new(@block_source, @context, self)
	end

	@input.each do |e|
	  @key_value_buffer.push(e)
	  e = nil
	end
	@key_value_buffer.each do |values|
	  values.each(&block)
	end
	@key_value_buffer = nil
      end

      def hash_key(e)
	@hash_proc.yield(e)
      end
    end


  end
end


