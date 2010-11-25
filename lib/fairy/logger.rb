# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  class Logger

    LOG_FILE = CONF.LOG_FILE

    FLUSH_INTERVAL = CONF.LOG_FLUSH_INTERVAL
    MARK_INTERVAL = CONF.LOG_MARK_INTERVAL
    LOG_ROTATE_INTERVAL = CONF.LOG_ROTATE_INTERVAL

    def initialize(path = LOG_FILE)
      @log_file_path = path

      open_log
      
      @mutex = Mutex.new
      @buffered = false
      
      start_flusher
    end

    def open_log
      begin
	@log_out = File.open(@log_file_path, "a+")
      rescue Errno::ENOENT
	ERR::Fail ERR::NoLogDir, @log_file_path
      rescue
	raise
      end
      @log_open_time = Time.now
      @log_out.puts @log_open_time.strftime("%Y/%m/%d %H:%M:%S -- LOGGER START --")
      @log_out.flush

      @marked_time = @log_open_time
    end

    def start_flusher
      Thread.start do
	loop do
	  sleep FLUSH_INTERVAL
	  @mutex.synchronize do
	    if @buffered
	      @log_out.flush
	      @buffered = false
	    end
	    now = Time.now 
	    if now - @marked_time > MARK_INTERVAL
	      @log_out.puts now.strftime("%Y/%m/%d %H:%M:%S -- MARK --")
	      @log_out.flush
	      @marked_time = now
	    end
	    if LOG_ROTATE_INTERVAL && now - @log_open_time > LOG_ROTATE_INTERVAL
	      log_rotate
	    end
	  end
	end
      end
    end

    def message(message)
      @mutex.synchronize do
	@log_out.puts message
	@buffered = true
      end
      nil
    end

    def messages(messages)
      @mutex.synchronize do
	messages.each do |m|
	  @log_out.puts m
	end
	@buffered = true
      end
      nil
    end
    DeepConnect.def_method_spec(self, "REF messages(DVAL)")

    def log_rotate
      @log_out.close
      log_back = "#{@log_file_path}.BAK-$$"
      File.rename(@log_file_path, log_back)
      open_log

      Thread.start do
	files = Dir.glob("#{@log_file_path}.[0-9]*").sort{|f1, f2| f2 <=> f1}
	while files.size  >= CONF.LOG_ROTATE_N
	  File.unlink(files.shift)
	end

	files.each do |f|
	  fn = f.sub(/\.[0-9]+/){|n| n.succ}
	  File.rename(f, fn)
	  if /.*\.gz$/ !~ fn
	    system("gzip #{fn}")
	  end
	end

	File.rename(log_back, "#{@log_file_path}.0")
      end
    end
  end
end
