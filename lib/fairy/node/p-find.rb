# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"

require "fairy/node/p-io-filter"
require "fairy/node/p-single-exportable"

module Fairy
  class PLocalFind<PSingleExportFilter
    Processor.def_export self

    def initialize(id, ntask, bjob, opts, block_source)
      super
      @block_source = block_source

      @findp = false
      @findp_mutex = Mutex.new
    end

    def basic_each(&block)
      @find_proc = BBlock.new(@block_source, @context, self)

      @input.each do |e|
	# 見つかっていたら空読み
	@findp_mutex.synchronize do
	  next if @findp
	  if !(@findp = @find_proc.yield(e))
	    next
	  elsif Import::CTLTOKEN_NULLVALUE === @findp
	    @findp = false
	    next
	  end
	  block.call e
	end
      end
    end

#     def start
#       super do
# 	@import.each do |e|
# 	  # 見つかっていたら空読み
# 	  @find_mutex.synchronize do
# 	    next if @find 
# 	    next unless find = @map_proc.yield(e)
# 	    @export.push e
# 	  end
# 	end
#       end
#     end

    def find_break
      @find_mutex.synchronize do
	@findp = true
      end
    end
  end


  class PFindResult<PIOFilter
    Processor.def_export self

    def initialize(*args)
      super

      @value = :__FAIRY_NO_VALUE__
      @value_mutex = Mutex.new
      @value_cv = XThread::ConditionVariable.new
    end

    def input=(input)
      super

      start do
	self.start_find
      end
    end

    alias super_each each

    def each(&block)
      block.call value
    end

    def value
      @value_mutex.synchronize do
	while @value == :__FAIRY_NO_VALUE__
	  @value_cv.wait(@value_mutex)
	end
	@value
      end
    end

    def start_find
      find = false
      @input.each do |e|
	# 最初の要素以外空読み
	next if find 
	find = e

	@value = find
	@value_cv.broadcast 
#	@export.push find
	# ちょっと気になる...
#	@export.push END_OF_STREAM

	@bjob.update_find
      end
    end
  end
end
