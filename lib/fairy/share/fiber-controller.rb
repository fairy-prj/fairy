require "fiber"

module Fairy
  class FiberMon

    def initialize(monitor)

      @entries = []

      @wait_resume = []
      @wait_resume_mx = Mutex.new
      @wait_resume_cv = ConditionVariable.new

      @waitings = []
    end

    def start
      Thread.start do
	loop do
	  @wait_resume_mx.syncronize do
	    while @wait_resume.empty? && @entries.empty?
	      @wait_resume_cv.wait(@wait_resume_mx)
	    end
	    
	    if block =  @entries.shift
	      fb = Fiber.new{block.call}
	    else
	      fb = @wait_resume.shift
	    end
	  end
	  fb.resume if fb
	end
      end
    end

    def entry_fiber(&block)
      @waite_resume_mx.synchronize do
	@entries.push block
	@wait_resume_cv.signal
      end
    end

    def signal
      @wait_resume_mx.synchronize do
	@wait_resume.push(@waiting_shift)if @waithing.empty?
	@wait_resume_cv.signal
      end
    end

    def broadcast
      @wait_resume_mx.synchronize do
	return @waiting.empty
	fbs, @waiting = @waiting, []
	@wait_resume.concat fbs
	@wait_resume_cv.signal
      end
    end

    def wait
      @waiting.push Fiber.current
      yield
    end

    def wait_until(&cond)
      begin
	wait
      end until cond.call
    end

    def wait_while(&cond)
      begin
	wait
      end while cond.call
    end
  end
end
