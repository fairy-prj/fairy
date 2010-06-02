
module Fairy

  class MarshaledQueue

    def initialize(policy, queues_mon = Monitor.new, queues_cv = queues_mon.new_cond)
      @policy = policy

      @chunk_size = CONF.MARSHAL_QUEUE_CHUNK_SIZE
      @min_chunk_no = CONF.MARSHAL_QUEUE_MIN_CHUNK_NO

      @push_queue = []
      @push_queue_mutex = Mutex.new
      
      @queues = []
      @queues_mon = queues_mon
      @queues_cv = queues_cv

      @pop_queue = nil
    end

    attr_accessor :fib_cv

    def push(e)
      @push_queue_mutex.synchronize do
	@push_queue.push e
	if @push_queue.size >= @min_chunk_no || 
	    e == :END_OF_STREAM || 
	    e == Import::SET_NO_IMPORT
	  @queues_mon.synchronize do
	    @push_queue.pop if e == :END_OF_STREAM
	    @queues.push Marshal.dump(@push_queue)
	    @queues.push e if e == :END_OF_STREAM

	    @push_queue = []
	    @queues_cv.broadcast
	  end
	end
      end
    end

    def push_all(buf)
      @push_queue_mutex.synchronize do
	@push_queue.concat buf
	if @push_queue.size > @min_chunk_no || 
	    @push_queue.last == :END_OF_STREAM
	  @queues_mon.synchronize do
	    @push_queue.pop if e == :END_OF_STREAM
	    @queues.push Marshal.dump(@push_queue)
	    @queues.push e if e == :END_OF_STREAM

	    @push_queue = []
	    @queues_cv.broadcast
	  end
	end
      end
    end

    def push_raw(raw)
      @push_queue_mutex.synchronize do
	@queues_mon.synchronize do
	  unless @push_queue.empty?
	    @queues.push Marshal.dump(@push_queue)
	    @push_queue = []
	  end
	  @queues.push raw
	  @queues_cv.broadcast
	end
      end
    end

    def pop
      while !@pop_queue || @pop_queue.empty?
	@queues_mon.synchronize do
	  raw = nil
	  @queues_cv.wait_until{raw = @queues.shift}
	  if raw == :END_OF_STREAM
	    @pop_queue = [raw]
	  else
	    @pop_queue = Marshal.load(raw)
	  end
	end
      end
      e = @pop_queue.shift
      @pop_queue = nil if @pop_queue.empty?
      e
    end

    def pop_all
      while !@pop_queue
	@queues_mon.synchronize do
	  raw = nil
	  @queues_cv.wait_until{raw = @queues.shift}
	  if raw == :END_OF_STREAM
	    @pop_queue = [raw]
	  else
	    @pop_queue = Marshal.load(raw)
	  end
	end
      end
      buf, @pop_queue = @pop_queue, nil
      buf
    end

    def pop_raw
      if @pop_queue && !@pop_queue.empty?
	ERR::Raise ERR::INTERNAL::MarshalQueueNotEmpty
      end
      
      pop_raw = nil
      while !pop_raw
	@queues_mon.synchronize do
	  @queues_cv.wait_until{pop_raw = @queues.shift}
	end
      end
      pop_raw
    end

  end


  class SizedMarshaledQueue<MarshaledQueue

    def initialize(policy, queues_mon = Monitor.new, queues_cv = queues_mon.new_cond)
      super
      @max_size = policy[:size]
      @max_size ||= CONF.SIZEDMARSHAL_QUEUE_MAX_CHUNK_NO

      @pop_cv = @queues_cv
      @push_cv = @queues_mon.new_cond
    end

    def push(e)
      @queues_mon.synchronize do
	@push_cv.wait_while{@queues.size > @max_size}
      end
      super
    end

    def push_all(buf)
      @queues_mon.synchronize do
	@push_cv.wait_while{@queues.size > @max_size}
      end
      super
    end

    def push_raw(raw)
      @queues_mon.synchronize do
	@push_cv.wait_while{@queues.size > @max_size}
      end
      super
    end

    def pop
      e = super
      @queues_mon.synchronize do
	@push_cv.broadcast if @queues.size <= @max_size
      end
      e
    end

    def pop_all
      buf = super
      @queues_mon.synchronize do
	@push_cv.broadcast if @queues.size <= @max_size
      end
      buf
    end

    def pop_raw
      raw = super
      @queues_mon.synchronize do
	@push_cv.broadcast if @queues.size <= @max_size
      end
      raw
    end
  end
  
end

