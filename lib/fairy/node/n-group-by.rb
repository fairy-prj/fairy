# encoding: UTF-8

require "fairy/node/n-filter"

module Fairy
  class NGroupBy<NFilter
    Processor.def_export self

    ST_ALL_IMPORTED = :ST_ALL_IMPORTED
    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
#      @key_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
#      @key_proc = @context.create_proc(@block_source)

      @exports = {}
      @exports_queue = Queue.new

      start_watch_exports
    end

    def add_export(key, export)
      @exports[key] = export
      @exports_queue.push [key, export]
    end

    def start
      super do
	@key_proc = BBlock.new(@block_source, @context, self)
	
	policy = @opts[:postqueuing_policy]
	begin
	  @import.each do |e|
#	    key = @key_proc.yield(e)
	    key = key(e)
	    export = @exports[key]
	    unless export
	      export = Export.new(policy)
	      export.add_key(key)
	      add_export(key, export)
	    end
	    export.push e
	  end
	rescue
	  Log::debug_exception(self)
	  raise
	ensure
	  @exports.each{|key, export| 
Log::debug(self, "G0 #{key}")	    
	    export.push END_OF_STREAM}
	  wait_export_finish
	end
      end
    end

    def key(e)
      @key_proc.yield(e)
    end

    def wait_export_finish

Log::debug(self, "G1")

      self.status = ST_ALL_IMPORTED

Log::debug(self, "G2")
      # すべての, exportのoutputが設定されるまで待っている
      # かなりイマイチ
      for key, export in @exports
	export.output
      end

Log::debug(self, "G3")
      # ここの位置が重要
      self.status = ST_WAIT_EXPORT_FINISH
      # ここもいまいち
Log::debug(self, "G4")
      for key,  export in @exports
Log::debug(self, "G4.WAIT #{key}")
	export.wait_finish
      end
Log::debug(self, "G5")
      self.status = ST_EXPORT_FINISH
    end

    def start_watch_exports
      Thread.start do
	loop do
	  key, export = @exports_queue.pop
	  notice_exports(key, export)
	end
      end
      nil
    end

    def notice_exports(key, export)
      @bjob.update_exports(key, export, self)
    end
  end

  class NMGroupBy<NFilter
    Processor.def_export self

    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
#      @key_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
#      @key_proc = @context.create_proc(@block_source)
      @key_proc = BBlock.new(@block_source, @context, self)

      @exports = {}
      @exports_queue = Queue.new

      start_watch_exports
    end

    def add_export(key, export)
      @exports[key] = export
      @exports_queue.push [key, export]
    end

    def start
      super do
	policy = @opts[:postqueuing_policy]
	@import.each do |e|
	  keys = @key_proc.yield(e)
	  keys = [keys] unless keys.kind_of?(Array)
	  
	  for key in keys 
	    export = @exports[key]
	    unless export
	      export = Export.new(policy)
	      export.add_key(key)
	      add_export(key, export)
	    end
	    export.push e
	  end
	end
	@exports.each{|key, export| export.push END_OF_STREAM}
	wait_export_finish
      end
    end

    def wait_export_finish

      # すべての, exportのoutputが設定されるまで待っている
      # かなりイマイチ
      for key, export in @exports
	export.output
      end

      # ここの位置が重要
      self.status = ST_WAIT_EXPORT_FINISH
      # ここもいまいち
      for key,  export in @exports
	export.wait_finish
      end
      self.status = ST_EXPORT_FINISH
    end

    def start_watch_exports
      Thread.start do
	loop do
	  key, export = @exports_queue.pop
	  notice_exports(key, export)
	end
      end
      nil
    end

    def notice_exports(key, export)
      @bjob.update_exports(key, export, self)
    end
  end
end


