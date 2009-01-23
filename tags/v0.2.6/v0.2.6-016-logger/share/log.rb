
require "deep-connect/future"

require "forwardable"
require "thread"


module Fairy
  class Log

    LEVELS = [:FATAL, :ERROR, :WARN, :INFO, :DEBUG]
    MESSAGE_LEVEL = :DEBUG
    
    PRINT_STDOUT = true

    def initialize
      @logger = nil
      @host = `hostname`.chomp
      @type = $0
      @pid = Process.pid

      @mutex = Mutex.new
    end

    @the_log = Log.new

    class<<self
      extend Forwardable

      def method_added(method)
	(class<<self;self;end).def_delegator :@the_log, method
      end
    end

    def logger=(logger)
      @logger = logger
    end

    def type=(type)
      @type = type
    end

    # Log::log(sender, format, args...)
    # Log::log(format, args,...)
    def log(sender, format=nil, *args, &block)
      raise "Loggerが設定されていません" unless @logger
      
      bt = caller(0).select{|l| /fairy.*(share\/log)|__FORWARDABLE__/ !~ l}
      bt.first =~ /\/([^\/]*\.rb):([0-9]+):in `(.*)'$/
      file_name = $1
      line_no = $2
      method = $3

      if sender.kind_of?(String)
	format = sender
	sender_type = "[UNDEF]"
      else
	sender_type = sender.class.name
      end

      t = Time.now.strftime("%m/%d %H:%M:%S")
      mes = sprintf("%s %s %s[%d-%s] %s[%s] %s#%s: ", 
		    t, @host, @type, @pid, 
		    Thread.current["name"],
		    file_name,
		    line_no,
		    sender_type,
		    method)
      if block_given?
	sio = StringIO.new(mes, "a+")
	yield sio
      else
	mes.concat sprintf(format, *args)
      end
      mes.chomp!
      stdout_puts mes if PRINT_STDOUT

      DeepConnect.future{@mutex.synchronize{@logger.message(mes)}}
    end
    alias stdout_puts puts
    alias puts log

    # Log::log_exception(sender, exception, level = :WARN)
    # Log::log_exception(exception, level = :WARN)
    def log_exception(sender, exception=nil)
      if sender.kind_of?(Exception)
	exception = sender
	sender = "dummy"
      end
      log(sender) do |sio|
	sio.puts exception
	for l in exception.back_trace
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
	alias_method method+"_exception", :log
      end
    end

    range = LEVELS[LEVELS.index(MESSAGE_LEVEL)+1.. -1]
    if range
      for level in range
	method = level.id2name.downcase
	alias_method method, :nop
	alias_method method+"_exception", :nop
      end
    end
  end
end
