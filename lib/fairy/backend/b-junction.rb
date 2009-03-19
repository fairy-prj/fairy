# encoding: UTF-8

require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"

module Fairy
  class BJunction<BFilter
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
      Thread.start do
	@input.each_export do |*export_node|
	  @port_queue.push export_node
	end
	@port_queue.push nil
      end
    end

    def each_export(&block)
      for exp, node in @port_queue
Log::debug(self, "%s %s", exp.to_s, node.to_s)
	block.call exp, node
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
