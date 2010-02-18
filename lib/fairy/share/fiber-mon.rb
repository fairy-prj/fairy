
require "monitor"

module Fairy
  class FiberMon
    include MonitorMixin

    def initialize
      super

#      @mon_mx = Mutex.new
#      @owner = nil
#      @mon_mxmx = Mutex.new

      @current = nil

      @entries = []

      @wait_resume = []
      @wait_resume_mx = Mutex.new
      @wait_resume_cv = ::ConditionVariable.new
    end

    attr_reader :current

    def start
      Thread.start do
	loop do
	  @wait_resume_mx.synchronize do
	    while @wait_resume.empty? && @entries.empty?
	      @wait_resume_cv.wait(@wait_resume_mx)
	    end
	    
	    if block =  @entries.shift
	      @current = Fiber.new{block.call(self)}
	    else
	      @current = @wait_resume.shift
	    end
	  end
	  @current.resume if @current
	  @current = nil
	end
      end
    end

    def entry_fiber(p0 = nil, &block)
      p0 = block if block
      @wait_resume_mx.synchronize do
	@entries.push p0
	@wait_resume_cv.signal
      end
    end
    alias entry entry_fiber

    def yield
      @wait_resume_mx.synchronize do
	@wait_resume.push @current
	@wait_resume_cv.signal
      end
      fiber_yield
    end

#     def synchronize(&block)
#       @mon_mxmx.synchronize do
# 	@mon_mx.lock
# 	@owner = Thread.current
#       end
#       begin
# 	block.call
#       ensure
# 	@mon_mxmx.synchronize do
# 	  @owner = nil
# 	  @mon_mx.unlock
# 	end
#       end
#     end

#     def fiber_yield
#       begin
# 	status = nil
# 	@mon_mxmx.synchronize do
# 	  if status = @owner == Thread.current && @mon_mx.locked?
# 	    @mon_mx.unlock
# 	    @owner = nil
# 	  end
# 	end
# 	Fiber.yield
#       rescue
# 	Log::debug_exception(self)
#       ensure
# 	@mon_mxmx.synchronize do
# 	  if status
# 	    @owner = Thread.current
# 	    @mon_mx.lock
# 	  end
# 	end
#       end
#     end

    def fiber_yield
      begin
	mon_check_owner
	count = mon_exit_for_cond
	@mon_mutex.unlock if count > 0
	begin
	  Fiber.yield
	ensure
	  @mon_mutex.lock if count > 0
	  mon_enter_for_cond(count)
	end
      rescue
	Fiber.yield
      end
    end

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

      def synchronize(&block)
	@waitings_mx.synchronize(&block)
      end

      def signal(&block)
	@waitings_mx.synchronize do
	  if block_given?
	    yield
	  end

	  if fb =  @waitings.shift
	    @mon.entry_wait_resume(fb)
	  end
	end
      end

      def broadcast(&block)
	@waitings_mx.synchronize do
	  if block_given?
	    yield
	  end

	  return if @waitings.empty?
	  fbs, @waitings = @waitings, []
	  @mon.entry_wait_resume(*fbs)
	end
      end

      def wait
	@waitings_mx.synchronize do
	  @waitings.push @mon.current
	end
	@mon.fiber_yield
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
