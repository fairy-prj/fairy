# encoding: UTF-8

require "fairy/node/n-filter"

module Fairy
  class NLocalFind<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
      @map_proc = BBlock.new(@block_source, @context, self)

      @find = false
      @find_mutex = Mutex.new
    end

    def start
      super do
	@import.each do |e|
	  # 見つかっていたら空読み
	  @find_mutex.synchronize do
	    next if @find 
	    next unless find = @map_proc.yield(e)
	    @export.push e
	  end
	end
      end
    end

    def find_break
      @find_mutex.synchronize do
	@find = true
      end
    end
  end


  class NFindResult<NSingleExportFilter
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

    def start
      super do
	find = false
	@import.each do |e|
	  # 最初の要素以外空読み
	  next if find 
	  find = e

	  @value = find
	  @value_cv.broadcast 
	  @export.push find
	  # ちょっと気になる...
	  @export.push END_OF_STREAM

	  @bjob.update_find
	end
      end
    end
  end
end
