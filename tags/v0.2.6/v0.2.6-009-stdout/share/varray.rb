
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

    def [](idx)
      case idx
      when Integer
	ary_idx, idx = index_on_arrays(idx)
	return arrays[ary_idx][idx]
      when Range
	raise TypeError, "そのクラスはサポートしていません(#{idx})"
      else
	raise TypeError, "そのクラスはサポートしていません(#{idx})"
      end
    end

    def []=(idx, val)
      case idx
      when Integer
	ary_idx, idx = index_on_arrays(idx)
	return arrays[ary_idx][idx]=val
      else
	raise TypeError, "そのクラスはサポートしていません(#{idx})"
      end
    end

    def each(&block)
      # set_arrayされるまでまっている.
      arrays.size.times do |idx|
	ary = nil
	@arrays_mutex.synchronize do
	  while @arrays[idx].nil?
	    @arrays_cv.wait(@arrays_mutex)
	  end
	  ary = @arrays[idx]
	end
	ary.each(&block)
      end
    end

    # arrays 操作
    def arrays
      @arrays_mutex.synchronize do
	while @arrays.nil?
	  @arrays_cv.wait(@arrays_mutex)
	end
	@arrays
      end
    end

    def set_arrays(array)
      @arrays_mutex.synchronize do
	@arrays = array
	@arrays_cv.broadcast
      end
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
	@arrays_cv.broadcast
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

    def index_on_arrays(idx)
      arrays.each_index do |ary_idx|
	ary_size = arrays[ary_idx].size
	if idx < ary_size
	  return ary_idx, idx
	end
	idx -= ary_size
      end
    end

  end
end

