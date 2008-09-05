
require "node/njob"
require "node/n-single-exportable"

module Fairy

  class NInject<NSingleExportFilter
    def initialize(processor, bjob, opts, block_source)
      super

      @init_value = :__FAIRY_NO_VALUE__
      if @opts.key?(:init_value)
	@init_value = @opts[:init_value]
      end
      @block_source = block_source
      @inject_proc = @context.create_proc(@block_source)
    end

    def start
      super do
	sum = @init_value
	@import.each do |e|
	  if sum == :__FAIRY_NO_VALUE__
	    sum = e
	  else
	    sum = @inject_proc.call(sum, e)
	  end
	end
	finish(sum)
      end
    end
  end

  class NLocalInject<NInject
    Processor.def_export self
    
    def finish(sum)
      @export.push sum
    end

  end

  class NWideInject<NInject
    Processor.def_export self

    def initialize(*args)
      super

      @value = :__FAIRY_NO_VALUE__
      @value_mutex = Mutex.new
      @value_cv = ConditionVariable.new
    end

    def value
      @value_mutex.synchronize do
	while @value == :__FAIRY_NO_VALUE__
	  @value_cv.wait(@value_mutex)
	end
	@value
      end
    end

    def finish(sum)
      @value = sum
      @value_cv.broadcast 
      if @export
	@export.push sum
      end
    end
  end
end
