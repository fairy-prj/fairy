# encoding: UTF-8

require "fairy/processor"

module Fairy
  class NTask
    Processor.def_export self

    END_OF_STREAM = :END_OF_STREAM

    ST_INIT = :ST_INIT
    ST_ACTIVATE = :ST_ACTIVATE
    ST_FINISH = :ST_FINISH

    def initialize(processor)
      Log::info self, "CREATE NTask"
      @processor = processor

      @njobs = []

      @status = ST_INIT
      @status_mutex = Mutex.new
      @status_cv = ConditionVariable.new
    end

    attr_reader :processor

    #
    # njob methods
    #
    def create_njob(njob_class_name, bjob, opts, *rests)
      klass = @processor.import(njob_class_name)
      njob = klass.new(self, bjob, opts, *rests)
      @njobs.push njob
      Log.debug(self, "Njob number of %d", @njobs.size)
      njob
    end

    def abort_running
      @status_mutex.synchronize do
	@njobs.last.abort_running unless [ST_INIT, ST_FINISH].include?(@status)
      end
    end

    #
    # status methods.
    #
    def status=(val)
      @status_mutex.synchronize do
	@status = val
	@status_cv.broadcast
      end
    end

    def start_watch_status
      # 初期状態通知
      notice_status(@status)

      Thread.start do
	old_status = nil
	loop do
	  @status_mutex.synchronize do
	    while old_status == @status
	      @status_cv.wait(@status_mutex)
	    end
	    old_status = @status
	    notice_status(@status)
	  end
	end
      end
      nil
    end

    def update_status(node, st)
      @status_mutex.synchronize do
	@status = st
	@status_cv.broadcast
      end
    end

    def notice_status(st)
      @processor.update_status(self, st)
    end

  end
end




