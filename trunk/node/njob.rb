
require "thread"

module Fairy

  class NJob

    END_OF_STREAM = :END_OF_STREAM

    ST_INIT = :ST_INIT
    ST_ACTIVATE = :ST_ACTIVATE
    ST_FINISH = :ST_FINISH

    def initialize(processor, bjob)
puts "INIT1:0"
      @processor = processor
      @bjob = bjob

      @status = ST_INIT
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new

      start_watch_status
    end

    attr_reader :processor

    def start(&block)
      Thread.start do
	self.status = ST_ACTIVATE
	begin
	  block.call
	ensure
	  self.status = ST_FINISH
	end
      end
    end

    def status=(val)
#      @status_mutex.synchronize do
	@status = val
	@status_cv.broadcast
#      end
    end

    def start_watch_status
      Thread.start do
	old_status = nil
	loop do
	  @status_mutex.synchronize do
	    while old_status == @status
	      @status_cv.wait(@status_mutex)
	    end
	    old_status = @status
	  end
	  notice_status(@status)
	end
      end
    end

    def start_watch_status0
      Thread.start do
	old_status = nil
	@status_mutex.synchronize do
	  loop do
	    while old_status == @status
	      @status_cv.wait(@status_mutex)
	    end
	    old_status = @status
	    notice_status(@status)
	  end
	end
      end
    end

    def notice_status(st)
      @bjob.update_status(self, st)
    end

  end
end
