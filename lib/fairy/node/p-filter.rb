# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "thread"
require "xthread"

require "fairy/node/p-task"

require "fairy/share/block-source"
require "fairy/share/port"

module Fairy

  class PFilter
    Processor.def_export self

    END_OF_STREAM = :END_OF_STREAM

    ST_INIT = :ST_INIT
    ST_ACTIVATE = :ST_ACTIVATE
    ST_FINISH = :ST_FINISH

    DeepConnect.def_single_method_spec(self, "REF new(DEFAULT, REF, REF, VAL, *DEFAULT)")

    def initialize(id, ntask, bjob, opts={}, *rests)
      @id = id
      @log_id = format("%s[%s]", self.class.name.sub(/Fairy::/, ''), @id)

      Log::info self, "CREATE NJOB: #{self.class}"
      @ntask = ntask
      @bjob = bjob
      @opts = opts

      @IGNORE_EXCEPTION = CONF.IGNORE_EXCEPTION_ON_FILTER
      @IGNORE_EXCEPTION = @opts[:ignore_exception] if @opts.key?(:ignore_exception)

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
      @no_cv = XThread::ConditionVariable.new

      @key = nil
      @key_mutex = Mutex.new
      @key_cv = XThread::ConditionVariable.new

      @status = ST_INIT
      @status_mon = processor.njob_mon.new_mon
      @status_cv = @status_mon.new_cv
      notice_status(@status)

      @terminate_mon = processor.njob_mon.new_mon

      @in_each = nil
      @in_each_mutex = Mutex.new

#      start_watch_status
    end

    attr_reader :id
    attr_reader :log_id

    attr_reader :IGNORE_EXCEPTION

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

      start_watch_status

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

	    @main_thread = nil
	    processor.njob_mon.entry terminate_proc
	    Log::info self, "FINISH PROCESSING: #{self.class}"
	  end
	rescue Exception
	  Log::error_exception(self)
	  handle_exception($!)
	  raise
	end
      }
      nil
    end

    def terminate_proc
      proc{|*dummy| terminate}
    end

    def terminate
      self.status = ST_FINISH
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
	  basic_each do |e|
	    case e
	    when Import::CTLTOKEN_NULLVALUE
	      next
	    else
	      begin
		block.call e
	      rescue
		if @IGNORE_EXCEPTION
		  Log::warn(self, "IGNORE_EXCEPTON!!")
		  Log::warn(self, "Entity: #{e.inspect}")
		  Log::error_exception(self)
		  next
		else
		  raise
		end
	      end
	    end
	  end
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
#Log::debugf(self, "STATUS_CHANGE: %s", val)
      @status_mon.synchronize do
	@status = val
	@status_cv.broadcast
      end
    end

    def start_watch_status
      # 初期状態通知
      notice_status(@status)

      processor.njob_mon.entry do
	@status_mon.synchronize do
	  old_status = nil
	  loop do
	    @status_cv.wait_while{old_status == @status}
	    old_status = @status
	    notice_status(@status)
	    break if @status == ST_FINISH
	  end
	end
      end
      nil
    end

    def notice_status(st)
      processor.njob_mon.entry do
	@bjob.update_status(self, st)
	@ntask.update_status(self, st)
      end
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

	@IGNORE_EXCEPTION = njob.IGNORE_EXCEPTION

#      Log::debugf(self, "CONTEXT: %s", eval("@Pool", self.binding))
      end

      attr_reader :IGNORE_EXCEPTION

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
