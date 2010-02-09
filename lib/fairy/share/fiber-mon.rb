require "fiber"

module Fairy
  class FiberMon

    def initialize(monitor)

      @entries = []

      @wait_resume = []
      @wait_resume_mx = Mutex.new
      @wait_resume_cv = ConditionVariable.new
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
    alias entry entry_fiber

    def new_cv
      ConditionVariable.new(self)
    end

    def entry_wait_resume(*fbs)
      @wait_resume_mx.synchronize do
	@wait_resume.concat(fbs)
	@wait_resume_cv.signal
      end
    end

    class ConditionVariable
      def initialize(monitor)
	@mon = monitor

	@waitings = []
	@waitings_mx = Mutex.new
      end

      def signal
	@waitings_mx.synchronize do
	  if fb =  @waitings.shift
	    @mon.entry_wait_resume(fb)
	  end
	end
      end

      def broadcast
	@waitings_mx.synchronize do
	  return if @waitings.empty?
	  fbs, @waitings = @waitings, []
	  @mon.entry_wait_resume(fbs)
	end
      end

      def wait
	@waitings_mx.synchronize do
	  @waitings.push Fiber.current
	end
	Fiber.yield
      end

      def wait_until(&cond)
	until cond.call
	  wait
	end 
      end

      def wait_while(&cond)
	while cond.call
	  wait
	end 
      end
    end
  end
end
