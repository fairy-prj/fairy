# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

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

Log::debug(self, "AA:0");
      @status = ST_INIT
Log::debug(self, "AA:1");
      @status_mon = processor.njob_mon.new_mon
Log::debug(self, "AA:2");
      @status_cv = @status_mon.new_cv
Log::debug(self, "AA:3");

      start_watch_status
Log::debug(self, "AA:4");
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
#Log::debugf(self, "KKKKKKKKKKLAS: %s", klass)
      njob = klass.new(njob_next_id, self, bjob, opts, *rests)
      @njobs.push njob
      Log.debugf(self, "Njob number of %d", @njobs.size)
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
Log::debug(self, "A:0");
      notice_status(@status)

Log::debug(self, "A:1");
      @processor.njob_mon.entry do
Log::debug(self, "A:2");
	@status_mon.synchronize do
Log::debug(self, "A:3");
	  old_status = nil
	  loop do
	    @status_cv.wait_while{old_status == @status}
	    old_status = @status
	    notice_status(@status)
	    break if @status == ST_FINISH
Log::debug(self, "A:4");
	  end
Log::debug(self, "A:5");
	end
Log::debug(self, "A:6");
      end
Log::debug(self, "A:7");
      nil
    end

    def update_status(node, st)
      self.status = st
    end

    def notice_status(st)
#      @status_mon.entry do
Log::debug(self, "A2:1");
	@processor.update_status(self, st)
Log::debug(self, "A2:2");
#      end
    end

    def to_s
      "#<#{self.class}:##{@id} #{@njobs.collect{|f| f.class.to_s}.join('-').gsub(/Fairy::/, "")}>"
    end

  end
end




