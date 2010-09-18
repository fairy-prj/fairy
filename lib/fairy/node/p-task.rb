# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/processor"

module Fairy
  class PTask
    Processor.def_export self

    END_OF_STREAM = :END_OF_STREAM

    ST_INIT = :ST_INIT
    ST_ACTIVATE = :ST_ACTIVATE
    ST_FINISH = :ST_FINISH

    def initialize(id, processor)
      @id = id
      Log::info self, "CREATE PTask"

      @processor = processor

      @njobs = []
      @njob_seq = -1
      @njob_seq_mutex = Mutex.new

      @status = ST_INIT
      @status_mon = processor.njob_mon
      @status_cv = @status_mon.new_cv

      start_watch_status
    end

    attr_reader :processor

    def log_id
      "PTask[#{@id}]"
    end

    #
    # njob methods
    #
    def njob_next_id
      @njob_seq_mutex.synchronize do
	@njob_seq += 1

	format("%02d-%02d", @id, @njob_seq)
      end
    end

    def create_njob(njob_class_name, bjob, opts, *rests)
      klass = @processor.import(njob_class_name)
#Log::debug(self, "KKKKKKKKKKLAS: %s", klass)
      njob = klass.new(njob_next_id, self, bjob, opts, *rests)
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




