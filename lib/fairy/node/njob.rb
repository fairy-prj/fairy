# encoding: UTF-8

require "thread"

require "fairy/processor"
require "fairy/node/ntask"
require "fairy/share/block-source"

module Fairy

  class NJob
    Processor.def_export self

    END_OF_STREAM = :END_OF_STREAM

    ST_INIT = :ST_INIT
    ST_ACTIVATE = :ST_ACTIVATE
    ST_FINISH = :ST_FINISH

    def initialize(ntask, bjob, opts={}, *rests)
      Log::info self, "CREATE NJOB: #{self.class}"
      @ntask = ntask
      @bjob = bjob
      @opts = opts

      @main_thread = nil

      @context = Context.new(self)
#       @begin_block = nil
#       if @opts[:BEGIN]
# 	@begin_block = BBlock.new(@opts[:BEGIN], @context, self)
#       end
#       @end_block = nil
#       if @opts[:END]
# 	@end_block = BBlock.new(@opts[:END], @context, self)
#       end

      @begin_block_source = nil
      if @opts[:BEGIN]
	@begin_block_source = @opts[:BEGIN]
	@begin_block_exec_p = false
      end
      @end_block_source = nil
      if @opts[:END]
	@end_block_source = @opts[:END]
      end

      @no = nil
      @no_mutex = Mutex.new
      @no_cv = ConditionVariable.new

      @key = nil
      @key_mutex = Mutex.new
      @key_cv = ConditionVariable.new

      @status = ST_INIT
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new

      @in_each = nil
      @in_each_mutex = Mutex.new

      start_watch_status
    end

    attr_reader :ntask
    def processor
      @ntask.processor
    end
    
    def no
#Log::debug(self, "XXXXXXXXXXXXXXXXXXXXXXXX")
#Log::debug_backtrace
      @no_mutex.synchronize do
	while !@no
	  @no_cv.wait(@no_mutex)
	end
	@no
      end
    end

    def no=(no)
      @no_mutex.synchronize do
	@no = no
	@no_cv.broadcast
	@no
      end
    end

    def key
#       @key_mutex.synchronize do
# 	while !@key
# 	  @key_cv.wait(@key_mutex)
# 	end
# 	@key
#       end
      @key
    end

    def key=(key)
#       @key_mutex.synchronize do
# 	@key = key
# 	@key_cv.broadcast
# 	@key
#       end
      @key=key
    end

    def start_export
      ERR::Raise ERR::INTERNAL::ShouldDefineSubclass
    end

    def start(&block)
      Log::info self, "START PROCESSING: #{self.class}"
      @main_thread = Thread.start{
	begin
	  self.status = ST_ACTIVATE
	  if @begin_block_source
	    bsource = BSource.new(@begin_block_source, @context, self)
	    bsource.evaluate
	  end
	  begin
	    basic_start &block
	  ensure
	    if @end_block_source
	      bsource = BSource.new(@end_block_source, @context, self)
	      bsource.evaluate
	    end
	    self.status = ST_FINISH
	  end
	rescue Exception
	  Log::error_exception(self)
	  handle_exception($!)
	  raise
	end
      }
      nil
    end

    def basic_start(&block)
      block.call
    end

    def each(&block)
      begin
	@in_each = true

	if @begin_block_source
	  bsource = BSource.new(@begin_block_source, @context, self)
	  bsource.evaluate
	  @begin_block_exec_p = true
	end
	begin
	  basic_each &block
	ensure
	  if @end_block_source
	    bsource = BSource.new(@end_block_source, @context, self)
	    bsource.evaluate
	  end
	end
      rescue @context.class::GlobalBreakFromOther
	Log::debug(self, "CAUGHT GlobalBreak From Other")
	global_break_from_other
	
      rescue LocalJumpError, @context.class::GlobalBreak
	Log::debug(self, "CAUGHT GlobalBreak")
	global_break
	
      rescue Exception
	Log::error_exception(self)
	handle_exception($!)
	raise
      ensure
	@in_each = false
      end
      nil
    end

    def next
      if @begin_block_source && @begin_block_exec_p
	bsource = BSource.new(@begin_block_source, @context, self)
	bsource.evaluate
	@begin_block_exec_p = true
      end
      begin
	ret = basic_next
      ensure
	if ret == :END_OF_STREAM
	  if @end_block_source
	    bsource = BSource.new(@end_block_source, @context, self)
	    bsource.evaluate
	  end
	end
      end
    end

    def global_break
      Thread.start{@bjob.break_running(self)}
#      Thread.current.exit
      self.status = ST_FINISH
      # 他のスレッドはとめていない
    end

    def global_break_from_other
      self.status = ST_FINISH
    end

    def break_running
      if @in_each
	@main_thread.raise @context.class::GlobalBreakFromOther
#      @main_thread.exit if @main_thread
	self.status = ST_FINISH
      end
      # 他のスレッドはとめていない
    end

    def abort_running
      @main_thread.exit if @main_thread
    end

    def status=(val)
      @status_mutex.synchronize do
	@status = val
	@status_cv.broadcast
      end
    end

    def start_watch_status
      # 初期状態通知
      notice_status(@status)

      Thread.start do
	old_status = nil
	loop do
	  @status_mutex.synchronize do
	    while old_status == @status
	      @status_cv.wait(@status_mutex)
	    end
	    old_status = @status
# puts "STATUS CHANGED: #{self} #{@status}"
	    notice_status(@status)
	  end
	end
      end
      nil
    end

    def start_watch_status0
      Thread.start do
	old_status = nil
	@status_mutex.synchronize do
	  loop do
	    while old_status == @status
	      @status_cv.wait(@status_mutex)
	    end
	    old_status = @status
	    notice_status(@status)
	  end
	end
      end
      nil
    end

    def notice_status(st)
      @bjob.update_status(self, st)
      @ntask.update_status(self, st)
    end

#     # block create
#     def create_block(source)
#       unless Fairy.const_defined?(:Pool)
# 	pool_dict = @bjob.pool_dict
# 	Fairy.const_set(:Pool, pool_dict)
#       end
#       eval("def Pool = Fairy::Pool", TOPLEVEL_BINDING)

#       binding = eval("def fairy_binding; binding; end; fairy_binding",
# 		      TOPLEVEL_BINDING, 
# 		      __FILE__,
# 		      __LINE__ - 3)
#     end

    def handle_exception(exp)
      @bjob.handle_exception(exp)
    end

    class Context
      def initialize(njob)
	@Pool = njob.instance_eval{@bjob.pool_dict}
#Log::debug(self, @Pool.peer_inspect)
	@JobPool = njob.instance_eval{@bjob.job_pool_dict}
	#      @Import = njob.instance_eval{@import}
	#      @Export = njob.instance_eval{@export}
	@__context = context

	@X = 1

#      Log::debug(self, "CONTEXT: %s", eval("@Pool", self.binding))
      end

      def context
	__binding
      end

      class GlobalBreak<Exception;end
      class GlobalBreakFromOther<Exception;end
#      class LocalBreak<Exception;end
      def global_break
	Thread.current.raise GlobalBreak
      end
      alias gbreak global_break

#       def local_break
# 	Thread.current.raise LocalBreak
#       end

      alias __binding binding
      def binding
	@__context
      end
      alias bind binding
    end
  end
end
