# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/n-single-exportable"

module Fairy
  class NBarrierMemoryBuffer<NSingleExportFilter
    Processor.def_export self

    ST_ALL_IMPORTED = :ST_ALL_IMPORTED

    def initialize(processor, bjob, opts=nil)
#      @export = Export.new()
      super

      @queuing_policy = CONF.BARRIER_MEMORY_BUFFERING_POLICY
      @queue = eval("#{@queuing_policy[:queuing_class]}").new(@queuing_policy)
    end

#     def input=(input)
#       unless @import
# 	@import = Import.new(Queue.new)
# 	@import.no=input.no
# 	@import.add_key(input.key)
# 	@import.set_log_callback do |n| 
# 	  Log::verbose(self, "IMPORT POP: #{n}")
# 	end

# 	start
#       end
#       self
#     end

    def input=(input)
      super
      start_buffering
    end

    def start_buffering
      Log::info self, "START  BUFFERING: #{self.class}"

      start_watch_status

      @main_thread = Thread.start {
	begin
	  self.status = ST_ACTIVATE
	  if @begin_block_source
	    bsource = BSource.new(@begin_block_source, @context, self)
	    bsource.evaluate
	  end
	  begin
	    basic_start{}
	  ensure
	    if @end_block_source
	      bsource = BSource.new(@end_block_source, @context, self)
	      bsource.evaluate
	    end

	    @main_thread = nil
	    Log::info self, "FINISH BUFFERING: #{self.class}"
	  end
	rescue Exception
	  Log::error_exception(self)
	  handle_exception($!)
	  raise
	end
      }
    end

    def basic_start(&block)
      Log::debug(self, "START")

      begin
	@input.each{|e| @queue.push e}
      ensure
	@queue.push :END_OF_STREAM
	self.status = ST_ALL_IMPORTED
      end
    end


    def basic_each(&block)
      @bjob.wait_export
      
      while (e = @queue.pop) != END_OF_STREAM
	block.call e
      end
    end

  end

end
