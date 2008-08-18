
module Fairy
  class VArray
    include Enumerable

    def self.output(fairy, opts)
      OutputVArray.output(fairy, opts)
    end

    def initialize
      @arrays = nil
      @arrays_mutex = Mutex.new
      @arrays_cv = ConditionVariable.new
    end

    def arrays
      @arrays_mutex.synchronize do
	while @arrays.nil?
	  @arrays_cv.wait(@arrays_mutex)
	end
	@arrays
      end
    end

    def set_arrays(array)
      @arrays = array
      @arrays_cv.broadcast
    end

    def arrays_size
      self.arrays.size
    end

    def arrays_put(idx, array)
      @arrays_mutex.synchronize do
	while @arrays.nil?
	  @arrays_cv.wait(@arrays_mutex)
	end
	@arrays[idx] = array
      end
    end

    def arrays_at(idx)
      @arrays_mutex.synchronize do
	while @arrays.nil? or @arrays[idx].nil?
	  @arrays_cv.wait(@arrays_mutex)
	end
	@arrays[idx]
      end
    end

    def each(&block)
      arrays # set_arrayされるまで待つ
      
      arrays.size.times do |idx|
	ary = nil
	@arrays_mutex.synchronize do
	  while @arrays.nil? or @arrays[idx].nil?
	    @arrays_cv.wait(@arrays_mutex)
	  end
	  ary = @arrays[idx]
	end
	ary.each(&block)
      end
    end
  end
end

