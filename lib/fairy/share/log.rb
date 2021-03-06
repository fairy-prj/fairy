# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "thread"
require "xthread"
require "stringio"

require "forwardable"

require "deep-connect/future"

module Fairy
  class Log

    LEVELS = [:FATAL, :ERROR, :WARN, :INFO, :VERBOSE, :DEBUG]
    MESSAGE_LEVEL = CONF.LOG_LEVEL
    
    def initialize
      @logger = nil
      @host = `hostname`.chomp
      @type = $0
      @pid = nil

      @export_thread = nil

      @mutex = Mutex.new

      @puts_mutex = Mutex.new

      @buffer = []
      @buffer_mutex = Mutex.new
      @buffer_cv = XThread::ConditionVariable.new

      set_local_output_dev
      
      start_exporter
    end

    def start_exporter
      @export_thread = Thread.start {
	loop do
	  buf = nil
	  @buffer_mutex.synchronize do
	    while @buffer.empty?
	      @buffer_cv.wait(@buffer_mutex)
	    end
	    buf = @buffer.dup
	    @buffer.clear
	  end
	  @logger.messages(buf)
	end
      }
    end

    def set_local_output_dev(dev = CONF.LOG_LOCAL_OUTPUT_DEV)
      case dev
      when nil
	@LOCAL_OUTPUT_DEV = nil
      when String, Symbol
	begin
	  @LOCAL_OUTPUT_DEV = eval(dev.to_s)
	rescue
	  Log::warn(self, "Can't set local output dev")
	  Log::warn(self, "Use old local output dev")
	end
      else
	@LOCAL_OUTPUT_DEV = dev
      end
    end

    @the_log = Log.new unless @the_log

    class<<self
      extend Forwardable

      def_delegator :@the_log, :set_local_output_dev

      def method_added(method)
	(class<<self;self;end).def_delegator :@the_log, method
      end
    end

    attr_accessor :logger
    attr_reader :host
    attr_accessor :type
    attr_accessor :pid
    attr_accessor :LOCAL_OUTPUT_DEV

    def stop_export
      @export_thread.exit
    end

    def log(sender, str = nil, &block)
      bt = caller(0).select{|l| /fairy.*(share\/log)|__FORWARDABLE__|forwardable/ !~ l}
      bt.first =~ /\/([^\/]*\.rb):([0-9]+):in `(.*)'$/
      file_name = $1
      line_no = $2
      method = $3

      if sender.kind_of?(String)
	str = sender
	sender_type = "[UNDEF]"
      else
	begin
	  sender_type = sender.log_id
	rescue
	  sender_type = sender.class.name.sub(/Fairy::/, "")
	end
      end

      time = Time.now
      prefix = time.strftime("%Y/%m/%d %H:%M:%S")
      prefix.concat sprintf(".%06d %s ", time.usec, @host)
      mes = sprintf("%s%s%s %s[%s] %s#%s: ", 
		    @type, 
		    @pid ? "\##{@pid}": "", 
		    Thread.current["name"] ? Thread.current["name"]: "",
		    file_name, line_no,
		    sender_type, method)
      if block_given?
	sio = StringIO.new(mes, "a+")
	yield sio
      else
	mes.concat str
      end
      mes.chomp!
      
      if @LOCAL_OUTPUT_DEV
	@puts_mutex.synchronize do
	  begin
	    @LOCAL_OUTPUT_DEV.local_stdout.puts mes
	  rescue
	    @LOCAL_OUTPUT_DEV.puts mes
	  end
	end
      end
      
      if @logger
	@buffer_mutex.synchronize do
	  @buffer.push prefix+mes
	  @buffer_cv.signal
	end
      else
      	$stdout.puts "****Loggerが設定されていません****"
      end
    end
    #alias stdout_puts puts
    alias puts log

    # Log::log(sender, format, args...)
    # Log::log(format, args,...)
    def logf(sender, format=nil, *args)
      log(sender, sprintf(format, *args))
    end
    alias printf logf

    # Log::log_exception(sender, exception, level = :WARN)
    # Log::log_exception(exception, level = :WARN)
    def log_exception(sender = $!, exception=$!)
      if sender.kind_of?(Exception)
	exception = sender
	sender = "UNDEF"
      end
      log(sender) do |sio|
	if exception.kind_of?(Exception)
	  sio.puts "#{exception.message}: #{exception.class}"
	  for l in exception.backtrace
	    sio.puts l
	  end
	else
	  sio.puts "Unknown exception rised!!(Exp=#{exception.inspect})"
	  sio.puts "Backtorace: "
	  log_backtrace(sender)
	end
      end
    end

    def log_backtrace(sender = nil)
      log(sender) do |sio|
	for l in caller(0)
	  sio.puts l
	end
      end
    end
    
    def nop(*args); end

    range = LEVELS[0..LEVELS.index(MESSAGE_LEVEL)]
    if range
      for level in range
	method = level.id2name.downcase
	alias_method method, :log
	alias_method method+"f", :logf
	alias_method method+"_exception", :log_exception
	alias_method method+"_backtrace", :log_backtrace
      end
    end

    range = LEVELS[LEVELS.index(MESSAGE_LEVEL)+1.. -1]
    if range
      for level in range
	method = level.id2name.downcase
	alias_method method, :nop
	alias_method method+"f", :nop
	alias_method method+"_exception", :nop
	alias_method method+"_backtrace", :nop
      end
    end

    if MESSAGE_LEVEL == :DEBUG
      def debug_p(*objs)
	for o in objs
	  log(self, o.inspect)
	end
      end
    else
      def debug_p(*args);end      
    end
  end
end
