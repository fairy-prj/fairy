
module Fairy
  class BlockSource
    def initialize(source)
      @source = source
      @backtrace = caller(1).select{|l| /fairy.*(share|job)/ !~ l}
      l = caller(1)[caller(1).index(backtrace.first)-1]
      @caller_method = (/in `(.*)'/.match(l))[1]
    end

    attr_reader :source
    attr_reader :backtrace
    attr_reader :caller_method
  end


  class BScript
    def initialize(block_source, context, exception_handler)
      @block_source = block_source.dc_deep_copy
      @context = context
      @exception_handler = exception_handler
    end

    def evaluate
      match = /^(.*):([0-9]+)/.match(@block_source.backtrace.first)

      begin
	$stdout.replace_stdout do
	  eval(@block_source.source, @context.bind, match[1], match[2].to_i)
	end
      rescue Exception
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end
	bt = $!.backtrace.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l}
	if bt.first
	  bt.first.sub!("bind", @block_source.caller_method)
	end
	bt.push *@block_source.backtrace.dc_deep_copy
	$!.set_backtrace(bt)
	@exception_handler.handle_exception($!)
      end
    end
  end
  BSource = BScript

  class BBlock
    def initialize(block_source, context, exception_handler)
      @block_source = block_source.dc_deep_copy
      @context = context
      @exception_handler = exception_handler

      match = /^(.*):([0-9]+)/.match(@block_source.backtrace.first)
      begin
	@block = eval("proc{#{@block_source.source}}", context.binding, match[1], match[2].to_i)
      rescue ScriptError
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end
	bt = @block_source.backtrace.dc_deep_copy
	$!.set_backtrace(bt)
	@njob.handle_exception($!)
	# ここの処理がイマイチ
      end
    end

    def yield(*args)
      begin
	if @block.respond_to?(:yield)
	  $stdout.replace_stdout do
	    @block.yield(*args)
	  end
	else
	  if args.size == 1 && args.first.kind_of?(Array)
	    args = args.first.to_a
	  end
	  $stdout.replace_stdout do
	    @block.call(*args)
	  end
	end
      rescue @context.class::GlobalBreak
	@njob.global_break

      rescue Exception
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end
	bt = $!.backtrace.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l}
	bt.first.sub!("bind", @block_source.caller_method)
	bt.push *@block_source.backtrace.dc_deep_copy
	$!.set_backtrace(bt)
	@exception_handler.handle_exception($!)
      end
    end
    alias call yield
  end
end

