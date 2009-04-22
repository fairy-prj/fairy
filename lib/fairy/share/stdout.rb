# encoding: UTF-8

require "thread"

module Fairy

  class Stdout
    def initialize(peer)
      @local_stdout = $stdout
      @peer = peer
      @threads = {}
      
      @mutex = Mutex.new

    end
    
    attr_reader :local_stdout

    def write(str)
      @mutex.synchronize do
	if @threads[Thread.current]
	  @peer.stdout_write(str)
	else
	  @local_stdout.write(str)
	end
      end
    end

    def replace_stdout(&block)
      @mutex.synchronize do
	@threads[Thread.current] = 0 unless @threads[Thread.current] 
	@threads[Thread.current] += 1
      end
      begin
	yield
      ensure
	@mutex.synchronize do
	  @threads[Thread.current] -= 1
	  @threads.delete(Thread.current) if @threads[Thread.current] == 0
	end
      end
    end

    def flush
      # do nothing
    end
  end
end

#def puts(str)
#  $stdout.write(str+"\n")
#end

