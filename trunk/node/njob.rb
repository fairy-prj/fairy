
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

      @context = Context.new(self)
      @begin_block = nil
      if @opts[:BEGIN]
	@begin_block = BBlock.new(@opts[:BEGIN], @context, self)
      end
      @end_block
      if @opts[:END]
	@end_block = BBlock.new(@opts[:END], @context, self)
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
      Thread.start do
	self.status = ST_ACTIVATE
	if @begin_block
	  @begin_block.call
	end
	begin
	  block.call
	ensure
	  if @end_block
	    @end_block.call
	  end
	  self.status = ST_FINISH
	end
      end
      nil
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
      end

#      def create_proc(block_source)
#	BBlock.new(block_source, binding)
#      end

      def bind
	binding
      end

    end

    class BBlock
      def initialize(block_source, context, njob)
	@block_source = block_source.dc_deep_copy
	@context = context
	@njob = njob

	match = /^(.*):([0-9]+)/.match(@block_source.backtrace.first)
#	puts "XXX:0 : #{@block_source.backtrace.first}"
#	puts "XXX: #{match.to_a}"

	@block = eval("proc{#{@block_source.source}}", context.bind, match[1], match[2].to_i)
      end

      def yield(*args)
	begin
# 	  if args.size == 1 && args.first.__deep_connect_reference? && args.first.kind_of?(Array)
# 	    args = args.first.to_a
# 	  end
	  if args.size == 1 && args.first.kind_of?(Array)
	    args = args.first.to_a
	  end

	  if @block.respond_to?(:yield)
	    @block.yield(*args)
	  else
# 	    if @block.arity == 1 
# 	      @block.call(args)
# 	    else
# 	      @block.call(*args)
# 	    end
	  end
	  @block.call(*args)
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
