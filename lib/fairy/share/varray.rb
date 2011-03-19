# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "xthread"

module Fairy
  class VArray
    include Enumerable

    def self.output(fairy, opts)
      OutputVArray.output(fairy, opts)
    end

    # size がまだ決まっていないときには nil を指定する
    def initialize(arrays_size)
      @arrays = []
      @arrays_size = arrays_size
      @arrays_mutex = Mutex.new
      @arrays_cv = XThread::ConditionVariable.new
    end

    def size
      size = 0
      arrays_each do |array|
	size += array.size
      end
      size
    end

    def [](idx)
      case idx
      when Integer
	ary_idx, idx = index_on_arrays(idx)
	return @arrays[ary_idx][idx]
      when Range
	ERR::Raise ERR::NoSupportClass, idx
      else
	ERR::Raise ERR::NoSupportClass, idx
      end
    end

    def []=(idx, val)
      case idx
      when Integer
	ary_idx, idx = index_on_arrays(idx)
	return @arrays[ary_idx][idx]=val
      else
	ERR::Raise ERR::NoSupportClass, idx
      end
    end

    def each(&block)
      # set_arrayされるまでまっている.
      arrays_size.times do |idx|
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
    def arrays_size
      @arrays_mutex.synchronize do
	while @arrays_size.nil?
	  @arrays_cv.wait(@arrays_mutex)
	end
	@arrays_size
      end
    end

    def arrays_size=(arrays_size)
      @arrays_mutex.synchronize do
	@arrays_size = arrays_size
	@arrays_cv.broadcast
      end
    end

#     def arrays
#       @arrays_mutex.synchronize do
# 	while @arrays_size.nil?
# 	  @arrays_cv.wait(@arrays_mutex)
# 	end
# 	@arrays
#       end
#     end

#    def set_arrays(array)
#      @arrays_mutex.synchronize do
#	@arrays = array
#	@arrays_cv.broadcast
#      end
#    end

    def arrays_put(idx, array)
      @arrays_mutex.synchronize do
	@arrays[idx] = array
	@arrays_cv.broadcast
      end
    end

    def arrays_at(idx)
      @arrays_mutex.synchronize do
	while @arrays[idx].nil?
	  @arrays_cv.wait(@arrays_mutex)
	end
	@arrays[idx]
      end
    end

    def index_on_arrays(idx)
      # array_size=されるまでまっている.
      arrays_size.times do |ary_idx|
	ary = nil
	@arrays_mutex.synchronize do
	  while !(ary = @arrays[ary_idx])
	    @arrays_cv.wait(@arrays_mutex)
	  end
	end
	ary_size = ary.size
	if idx < ary_size
	  return ary_idx, idx
	end
	idx -= ary_size
      end
    end

    def arrays_each(&block)
      # array_size=されるまでまっている.
      arrays_size.times do |idx|
	ary = nil
	@arrays_mutex.synchronize do
	  while @arrays[idx].nil?
	    @arrays_cv.wait(@arrays_mutex)
	  end
	  ary = @arrays[idx]
	end
	yield ary
      end
    end
  end
end

