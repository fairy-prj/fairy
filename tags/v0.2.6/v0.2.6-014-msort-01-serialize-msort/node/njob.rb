
require "thread"

require "processor"
require "share/block-source"

module Fairy

  class NJob
    Processor.def_export self

    END_OF_STREAM = :END_OF_STREAM

    ST_INIT = :ST_INIT
    ST_ACTIVATE = :ST_ACTIVATE
    ST_FINISH = :ST_FINISH

    def initialize(processor, bjob, opts={}, *rests)
      puts "CREATE NJOB: #{self.class}"
      @processor = processor
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
      end
      @end_block_source = nil
      if @opts[:END]
	@end_block_source = @opts[:END]
      end

      @no = nil
      @no_mutex = Mutex.new
      @no_cv = ConditionVariable.new

      @status = ST_INIT
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new

      start_watch_status
    end

    attr_reader :processor
    
    def no=(no)
      @no = no
      @no_cv.broadcast
      @no
    end

    def no
      @no_mutex.synchronize do
	while !@no
	  @no_cv.wait(@no_mutex)
	end
	no
      end
    end

    def start(&block)
#      puts "START NJOB: #{self.class}"
      @main_thread = Thread.start{
	self.status = ST_ACTIVATE
	if @begin_block_source
	  bsource = BSource.new(@begin_block_source, @context, self)
	  bsource.evaluate
	end
	begin
	  block.call
	ensure
	  if @end_block_source
	    bsource = BSource.new(@end_block_source, @context, self)
	    bsource.evaluate
	  end
	  self.status = ST_FINISH
	end
      }
      nil
    end

    def global_break
      Thread.start{@bjob.break_running(self)}
      Thread.current.exit
      self.status = ST_FINISH
      # 他のスレッドはとめていない
    end

    def break_running
      @main_thread.exit
      self.status = ST_FINISH
      # 他のスレッドはとめていない
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
      @processor.update_status(self, st)
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
	@JobPool = njob.instance_eval{@bjob.job_pool_dict}
	#      @Import = njob.instance_eval{@import}
	#      @Export = njob.instance_eval{@export}
	@binding = context
      end

      def context
	binding
      end

#      def create_proc(block_source)
#	BBlock.new(block_source, binding)
#      end

      class GlobalBreak<Exception;end
      def global_break
	Thread.current.raise GlobalBreak
      end
      alias gbreak global_break

      def bind
	@binding
      end
    end

    class BSource
      def initialize(block_source, context, njob)
	@block_source = block_source.dc_deep_copy
	@context = context
	@njob = njob
      end

      def evaluate
	match = /^(.*):([0-9]+)/.match(@block_source.backtrace.first)

	begin
	  eval(@block_source.source, @context.bind, match[1], match[2].to_i)
	rescue Exception
	  puts "Warn: Exception raised:"
	  puts $!
	  for l in $@
	    puts "\t#{l}"
	  end
	  bt = $!.backtrace.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l}
	  if bt.first
	    bt.first.sub!("bind", @block_source.caller_method)
	  end
	  bt.push *@block_source.backtrace.dc_deep_copy
	  $!.set_backtrace(bt)
	  @njob.handle_exception($!)
	end
      end
    end

    class BBlock
      def initialize(block_source, context, njob)
	@block_source = block_source.dc_deep_copy
	@context = context
	@njob = njob

	match = /^(.*):([0-9]+)/.match(@block_source.backtrace.first)
	begin
	  @block = eval("proc{#{@block_source.source}}", context.bind, match[1], match[2].to_i)
	rescue ScriptError
	  puts "Warn: Exception raised:"
	  puts $!
	  for l in $@
	    puts "\t#{l}"
	  end
#	  bt = $!.backtrace.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l}
#	  bt.first.sub!("bind", @block_source.caller_method)
	  bt = @block_source.backtrace.dc_deep_copy
	  $!.set_backtrace(bt)
	  @njob.handle_exception($!)
	  # ここの処理がイマイチ
	end
      end

      def yield(*args)
	begin
# 	  if args.size == 1 && args.first.__deep_connect_reference? && args.first.kind_of?(Array)
# 	    args = args.first.to_a
# 	  end

	  if @block.respond_to?(:yield)
	    $stdout.replace_stdout do
	      @block.yield(*args)
	    end
	  else
# 	    if @block.arity == 1 
# 	      @block.call(args)
# 	    else
# 	      @block.call(*args)
# 	    end
	    if args.size == 1 && args.first.kind_of?(Array)
	      args = args.first.to_a
	    end
	    $stdout.replace_stdout do
	      @block.call(*args)
	    end
	  end
	rescue Context::GlobalBreak
	  @njob.global_break

	rescue Exception
	  puts "Warn: Exception raised:"
	  puts $!
	  for l in $@
	    puts "\t#{l}"
	  end
	  bt = $!.backtrace.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l}
	  bt.first.sub!("bind", @block_source.caller_method)
	  bt.push *@block_source.backtrace.dc_deep_copy
	  $!.set_backtrace(bt)
	  @njob.handle_exception($!)
	end
      end

      alias call yield
    end
  end


end
