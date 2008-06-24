

require "thread"
module Fairy
  class Reference
    class NullValue;end
    NULL_VALUE = NullValue.new

    def initialize
      @value = NULL_VALUE
      @value_mutex = Mutex.new
      @value_cv = ConditionVariable.new
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
	@value_cv.signal
      end
    end
  end
end
