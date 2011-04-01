# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  class BlockSource
    def initialize(source)
      @source = source
      @backtrace = caller(1).select{|l| /fairy.*(share|job)/ !~ l}
      l = caller(1)[caller(1).index(backtrace.first)-1]
#Log::debug_p(l)
      @caller_method = (/in `(.*)'/.match(l))[1]
#Log::debug_p(@caller_method)
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
	bt = $!.backtrace
	bt = bt.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l} unless CONF.DEBUG_FULL_BACKTRACE
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
	@exception_handler.handle_exception($!)
	# ここの処理がイマイチ
      end
    end

    def yield19(*args)
      begin
	$stdout.replace_stdout do
	  @block.yield(*args)
	end

      rescue LocalJumpError, 
	  @context.class::GlobalBreak, 
	  @context.class::GlobalBreakFromOther
	Log::debug(self, "CAUGHT GlobalBreak")
	raise

      rescue
	if @context.IGNORE_EXCEPTION
	  Log::warn(self, "IGNORE_EXCEPTON!!")
	  Log::warn(self, "Block Parameters: #{args.inspect}")
	end
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end

	if @context.IGNORE_EXCEPTION
	  return Import::TOKEN_NULLVALUE
	else
	  bt = $!.backtrace
	  bt = bt.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l} unless CONF.DEBUG_FULL_BACKTRACE
	  unless bt.empty?
	    bt.first.sub!("bind", @block_source.caller_method)
	  end
	  bt.push *@block_source.backtrace.dc_deep_copy
	  $!.set_backtrace(bt)

	  @exception_handler.handle_exception($!)
	end

      rescue Exception
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end
	bt = $!.backtrace
	bt = bt.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l} unless CONF.DEBUG_FULL_BACKTRACE
	bt.first.sub!("bind", @block_source.caller_method)
	bt.push *@block_source.backtrace.dc_deep_copy
	$!.set_backtrace(bt)

	@exception_handler.handle_exception($!)
      end
    end

    def yield19_no_use_stdout(*args)
      begin

	@block.yield(*args)

      rescue LocalJumpError, 
	  @context.class::GlobalBreak,
	  @context.class::GlobalBreakFromOther
	Log::debug(self, "CAUGHT GlobalBreak")
	raise
#	@exception_handler.global_break

      rescue
	if @context.IGNORE_EXCEPTION
	  Log::warn(self, "IGNORE_EXCEPTON!!")
	  Log::warn(self, "Block Parameters: #{args.inspect}")
	end
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end

	if @context.IGNORE_EXCEPTION
	  return Import::TOKEN_NULLVALUE
	else
	  bt = $!.backtrace
	  bt = bt.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l} unless CONF.DEBUG_FULL_BACKTRACE
	  bt.first.sub!("bind", @block_source.caller_method)
	  bt.push *@block_source.backtrace.dc_deep_copy
	  $!.set_backtrace(bt)

	  @exception_handler.handle_exception($!)
	end

      rescue Exception
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end
	bt = $!.backtrace
	bt = bt.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l} unless CONF.DEBUG_FULL_BACKTRACE
	bt.first.sub!("bind", @block_source.caller_method)
	bt.push *@block_source.backtrace.dc_deep_copy
	$!.set_backtrace(bt)

	@exception_handler.handle_exception($!)
      end
    end

    def yield18(*args)
      begin
	if args.size == 1 && args.first.kind_of?(Array)
	  args = args.first.to_a
	end
	$stdout.replace_stdout do
	  @block.call(*args)
	end

      rescue LocalJumpError, @context.class::GlobalBreak
	Log::debug(self, "CAUGHT GlobalBreak")
	@exception_handler.global_break

      rescue Exception
	Log::warn(self) do |sio|
	  sio.puts "Warn: Exception raised:"
	  sio.puts $!
	  for l in $@
	    sio.puts "\t#{l}"
	  end
	end
	bt = $!.backtrace
	bt = bt.select{|l| /fairy.*(share|job|backend|node|processor|controller)|deep-connect|__FORWARDABLE__|bin.*processor/ !~ l} unless CONF.DEBUG_FULL_BACKTRACE
	bt.first.sub!("bind", @block_source.caller_method)
	bt.push *@block_source.backtrace.dc_deep_copy
	$!.set_backtrace(bt)

	@exception_handler.handle_exception($!)
      end
    end

    if proc{}.respond_to?(:yield)
      if CONF.BLOCK_USE_STDOUT
	alias yield yield19
      else
	alias yield yield19_no_use_stdout
      end
    else
      alias yield yield18
    end
    alias call yield
  end
end

