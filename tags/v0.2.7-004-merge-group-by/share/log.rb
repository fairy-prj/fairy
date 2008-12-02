
require "thread"
require "stringio"

require "forwardable"

require "deep-connect/future"

module Fairy
  class Log

    LEVELS = [:FATAL, :ERROR, :WARN, :INFO, :DEBUG]
    MESSAGE_LEVEL = CONF.LOG_LEVEL
    
    PRINT_STDOUT = true

    def initialize
      @logger = nil
      @host = `hostname`.chomp
      @type = $0
      @pid = nil

      @mutex = Mutex.new
    end

    @the_log = Log.new

    class<<self
      extend Forwardable

      def method_added(method)
	(class<<self;self;end).def_delegator :@the_log, method
      end
    end

    attr_accessor :logger
    attr_accessor :type
    attr_accessor :pid

    # Log::log(sender, format, args...)
    # Log::log(format, args,...)
    def log(sender, format=nil, *args, &block)
      bt = caller(0).select{|l| /fairy.*(share\/log)|__FORWARDABLE__/ !~ l}
      bt.first =~ /\/([^\/]*\.rb):([0-9]+):in `(.*)'$/
      file_name = $1
      line_no = $2
      method = $3

      if sender.kind_of?(String)
	format = sender
	sender_type = "[UNDEF]"
      else
	sender_type = sender.class.name.sub(/Fairy::/, "")
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
	mes.concat sprintf(format, *args)
      end
      mes.chomp!
      stdout_puts mes if PRINT_STDOUT

      if @logger
	DeepConnect.future{@mutex.synchronize{@logger.message(prefix+mes)}} 
      else
	stdout_puts "****Logger�����ꤵ��Ƥ��ޤ���****"
      end
    end
    alias stdout_puts puts
    alias puts log

    # Log::log_exception(sender, exception, level = :WARN)
    # Log::log_exception(exception, level = :WARN)
    def log_exception(sender = $!, exception=$!)
      if sender.kind_of?(Exception)
	exception = sender
	sender = "UNDEF"
      end
      log(sender) do |sio|
	sio.puts exception
	for l in exception.backtrace
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
	alias_method method+"_exception", :log_exception
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

    if MESSAGE_LEVEL == :DEBUG
      def debug_p(*objs)
	for o in objs
	  log(o.inspect)
	end
      end
    else
      def debug_p(*args);end      
    end
  end
end
