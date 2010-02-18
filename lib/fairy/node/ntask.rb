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
      @status_mon = processor.njob_mon
      @status_cv = @status_mon.new_cv

      start_watch_status
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
    DeepConnect.def_method_spec(self, "REF create_njob(VAL, REF, VAL, *DEFAULT)")

    def abort_running
      @status_mon.synchronize do
	@njobs.last.abort_running unless [ST_INIT, ST_FINISH].include?(@status)
      end
    end

    #
    # status methods.
    #
    def status=(val)
      @status_mon.synchronize do
	@status = val
	@status_cv.broadcast
      end
    end

    def start_watch_status
      # 初期状態通知
      notice_status(@status)

      @processor.njob_mon.entry do
	@status_mon.synchronize do
	  old_status = nil
	  loop do
	    @status_cv.wait_while{old_status == @status}
	    old_status = @status
	    notice_status(@status)
	    break if @status == ST_FINISH
	  end
	end
      end
      nil
    end

    def update_status(node, st)
      self.status = st
    end

    def notice_status(st)
#      @status_mon.entry do
	@processor.update_status(self, st)
#      end
    end

  end
end




