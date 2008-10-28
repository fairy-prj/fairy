

module Fairy
  class Logger

    LOG_FILE = "/tmp/fairy/log"

    FLUSH_INTERVAL = 10

    def initialize(path = LOG_FILE)
      @log_out = File.open(LOG_FILE, "a+")
      
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
    end
  end
end
