

module Fairy
  class Logger

    LOG_FILE = "/tmp/fairy/log"

    def initialize(path = LOG_FILE)
      @log_out = File.open(LOG_FILE, "a+")
      @mutex = Mutex.new
    end

    def message(message)
      @mutex.synchronize{@log_out.puts message}
    end
  end
end
