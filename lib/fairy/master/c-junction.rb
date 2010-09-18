# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"

module Fairy
  class CJunction<CIOFilter
    Controller.def_export self

    def initialize(controller, opts)
      super
      @port_queue = PortQueue.new
    end

    def input=(input)
      @input = input
      start
    end

    def start
      Log::debug self, "START CONNECTING: #{self}"
      Thread.start do
	@input.each_assigned_filter do |input_filter|
	  @port_queue.push input_filter
	end
	@port_queue.push nil
      end
    end

    def each_assigned_filter(&block)
      for input_filter in @port_queue
#Log::debug(self, "%s %s", exp.to_s, node.to_s)
	block.call input_filter
      end
    end

    class PortQueue<DelegateClass(Queue)
      include Enumerable

      def initialize
	super(Queue.new)
      end

      def each
	while e = pop
	  yield e
	end
      end
    end
  end
end
