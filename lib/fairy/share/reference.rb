# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "thread"
require "xthread"

module Fairy
  class Reference
    class NullValue;end
    NULL_VALUE = NullValue.new

    def initialize
      @value = NULL_VALUE
      @value_mutex = Mutex.new
      @value_cv = XThread::ConditionVariable.new
    end

    def value
      @value_mutex.synchronize do
	while @value == NULL_VALUE
	  @value_cv.wait(@value_mutex)
	end
      end
      @value
    end

    def value=(v)
      @value_mutex.synchronize do
	@value = v
	@value_cv.broadcast
      end
    end

    def arrived?
      @value_mutex.synchronize do
	@value != NULL_VALUE
      end
    end

    def wait_arrived
      value
    end
  end
end
