# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  class Logger

    LOG_FILE = CONF.LOG_FILE

    FLUSH_INTERVAL = CONF.LOG_FLUSH_INTERVAL

    def initialize(path = LOG_FILE)
      begin
	@log_out = File.open(path, "a+")
      rescue Errno::ENOENT
	ERR::Fail ERR::NoLogDir, path
      rescue
	raise
      end
      
      @mutex = Mutex.new
      @buffered = false

      start_flusher
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
  end
end
