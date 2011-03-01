# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"

require "fairy/node/p-io-filter"

module Fairy
  class PBasicGroupBy<PIOFilter
    Processor.def_export self

    ST_ALL_IMPORTED = :ST_ALL_IMPORTED
    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(id, ntask, bjob, opts, block_source)
      super
      @block_source = block_source

      @exports = {}
      @exports_queue = XThread::Queue.new
      
      @counter = {}

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
	hash_opt = @opts[:grouping_optimize]
	hash_opt = CONF.GROUP_BY_GROUPING_OPTIMIZE if hash_opt.nil?
	
	if hash_opt
	  @key_proc = eval("proc{#{@block_source.source}}", @context.binding)
	else
	  @key_proc = BBlock.new(@block_source, @context, self)
	end
	
	policy = @opts[:postqueuing_policy]
	begin
	  @input.each do |e|
	    key = hash_key(e)
	    if Import::CTLTOKEN_NULLVALUE === key
	      next
	    end
	    export = @exports[key]
	    unless export
	      export = Export.new(policy)
	      export.njob_id = @id
	      export.add_key(key)
	      add_export(key, export)
	      @counter[key] = 0
	    end
	    export.push e
	    @counter[key] += 1
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

    def terminate
      @wait_cv = @terminate_mon.new_cv
      wait_export_finish
      super
    end

    def hash_key(e)
      @key_proc.yield(e)
    end

    def wait_export_finish

      Log::debug(self, "G1")

      self.status = ST_ALL_IMPORTED

      Log::debug(self, "G2")
      # ここの位置が重要
      self.status = ST_WAIT_EXPORT_FINISH
      # ここもいまいち
      Log::debug(self, "G3")
      @exports.each_pair do |key, export|
	Log::debug(self, "G3.WAIT #{key}")
	export.fib_wait_finish(@wait_cv)
      end
      Log::debug(self, "G4")
      self.status = ST_EXPORT_FINISH
    end

  end

  class PBasicMGroupBy<PIOFilter
    Processor.def_export self

    ST_ALL_IMPORTED = :ST_ALL_IMPORTED
    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(id, ntask, bjob, opts, block_source)
      super
      @block_source = block_source
#      @key_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
#      @key_proc = @context.create_proc(@block_source)
      @key_proc = BBlock.new(@block_source, @context, self)

      @exports = {}
      @exports_queue = XThread::Queue.new

#      start_watch_exports
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
	hash_opt = @opts[:grouping_optimize]
	hash_opt = CONF.GROUP_BY_GROUPING_OPTIMIZE if hash_opt.nil?

	if hash_opt
	  @key_proc = eval("proc{#{@block_source.source}}", @context.binding)
	else
	  @key_proc = BBlock.new(@block_source, @context, self)
	end

	policy = @opts[:postqueuing_policy]
        begin
          @input.each do |e|
            keys = @key_proc.yield(e)
            keys = [keys] unless keys.kind_of?(Array)
            
            for key in keys 
	      if Import::CTLTOKEN_NULLVALUE === key
		next
	      end
              export = @exports[key]
              unless export
                export = Export.new(policy)
		export.njob_id = @id
                export.add_key(key)
                add_export(key, export)
              end
              export.push e
            end
          end
        rescue
	  Log::debug_exception(self)
	  raise
        ensure
          @exports_queue.push nil
          @exports.each_pair do |key, export| 
	    next unless export
	    export.push END_OF_STREAM
	  end
        end
      end
    end

    def terminate
      @wait_cv = @terminate_mon.new_cv
      wait_export_finish
      super
    end


#     def start
#       super do
# 	policy = @opts[:postqueuing_policy]
# 	@import.each do |e|
# 	  keys = @key_proc.yield(e)
# 	  keys = [keys] unless keys.kind_of?(Array)
	  
# 	  for key in keys 
# 	    export = @exports[key]
# 	    unless export
# 	      export = Export.new(policy)
# 	      export.add_key(key)
# 	      add_export(key, export)
# 	    end
# 	    export.push e
# 	  end
# 	end
# 	@exports.each{|key, export| export.push END_OF_STREAM}
# 	wait_export_finish
#       end
#     end

    def wait_export_finish

Log::debug(self, "G1")
      self.status = ST_ALL_IMPORTED

Log::debug(self, "G2")
      # すべての, exportのoutputが設定されるまで待っている
      # かなりイマイチ
#      for key, export in @exports
#	export.output
#      end

      # ここの位置が重要
      self.status = ST_WAIT_EXPORT_FINISH
      # ここもいまいち
Log::debug(self, "G3")
      @exports.each_pair do |key, export|
	next unless export
Log::debug(self, "G4.WAIT #{key}")
	export.fib_wait_finish(@wait_cv)
      end
Log::debug(self, "G5")
      self.status = ST_EXPORT_FINISH
    end

#     def start_watch_exports
#       Thread.start do
# 	loop do
# 	  key, export = @exports_queue.pop
# 	  notice_exports(key, export)
# 	end
#       end
#       nil
#     end

#     def notice_exports(key, export)
#       @bjob.update_exports(key, export, self)
#     end
  end
end


