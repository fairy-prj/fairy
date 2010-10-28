# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-filter"
require "fairy/node/p-single-exportable"

module Fairy

  class PInject<PIOFilter
    def initialize(id, processor, bjob, opts, block_source)
      super

      @init_value = :__FAIRY_NO_VALUE__
      if @opts.key?(:init_value)
	@init_value = @opts[:init_value]
      end
      @block_source = block_source
#      @inject_proc = @context.create_proc(@block_source)
    end

    def basic_each(&block)
      @inject_proc = BBlock.new(@block_source, @context, self)
      sum = @init_value
      @input.each do |e|
	if sum == :__FAIRY_NO_VALUE__
	  sum = e
	else
	  if Import::CTLTOKEN_NULLVALUE === (v = @inject_proc.yield(sum, e))
	    next
	  end
	  sum = v
	end
      end
      finish(sum, &block)
    end

#     def start
#       super do
# 	@inject_proc = BBlock.new(@block_source, @context, self)
# 	sum = @init_value
# 	@import.each do |e|
# 	  if sum == :__FAIRY_NO_VALUE__
# 	    sum = e
# 	  else
# 	    sum = @inject_proc.yield(sum, e)
# 	  end
# 	end
# 	finish(sum)
#       end
  end

  class PLocalInject<PInject
    include PSingleExportable

    Processor.def_export self
    
    def finish(sum, &block)
      block.call sum
    end

  end

  class PWideInject<PInject
    Processor.def_export self

    def initialize(*args)
      super

      @value = :__FAIRY_NO_VALUE__
      @value_mutex = Mutex.new
      @value_cv = ConditionVariable.new
    end

    def input=(input)
      super

      start do
	self.super_each{}
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
    DeepConnect.def_method_spec(self, "DVAL value")
    
    def finish(sum, &block)
      @value = sum
      @value_cv.broadcast 
      if block
	block.call sum
      end
    end
  end
end
