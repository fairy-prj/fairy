
require "thread"

module Fairy

  class NJob

    END_OF_STREAM = :END_OF_STREAM

    ST_INIT = :ST_INIT
    ST_ACTIVATE = :ST_ACTIVATE
    ST_FINISH = :ST_FINISH

    def initialize(processor, bjob)
#      puts "CREATE NJOB: #{self.class}"
      @processor = processor
      @bjob = bjob

      @status = ST_INIT
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new

      @context = Context.new(self)

      start_watch_status
    end

    attr_reader :processor

    def start(&block)
#      puts "START NJOB: #{self.class}"
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
      @status_mutex.synchronize do
	@status = val
	@status_cv.broadcast
      end
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

#     # block create
#     def create_block(source)
#       unless Fairy.const_defined?(:Pool)
# 	pool_dict = @bjob.pool_dict
# 	Fairy.const_set(:Pool, pool_dict)
#       end
#       eval("def Pool = Fairy::Pool", TOPLEVEL_BINDING)

#       binding = eval("def fairy_binding; binding; end; fairy_binding",
# 		      TOPLEVEL_BINDING, 
# 		      __FILE__,
# 		      __LINE__ - 3)
#     end

  end

  class Context
    def initialize(njob)
      @Pool = njob.instance_eval{@bjob.pool_dict}
      @JobPool = njob.instance_eval{@bjob.job_pool_dict}
#      @Import = njob.instance_eval{@import}
#      @Export = njob.instance_eval{@export}
    end

    def create_proc(source)
      p source
      eval("proc{#{source}}", binding)
    end
  end

end