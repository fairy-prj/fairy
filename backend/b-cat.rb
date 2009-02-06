# encoding: UTF-8

require "delegate"

require "thread"

require "backend/bjob"
require "backend/b-filter"

module Fairy
  class BCat<BFilter
    Controller.def_export self

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(controller, opts, others)
      super
      
      @others = others
      @export_node_pairs_queues = nil
      @export_node_pairs_queues_mutex = Mutex.new
      @export_node_pairs_queues_cv = ConditionVariable.new
    end

    def input=(input)
      @input = input
      start_get_exports
    end

    def start_get_exports
      @export_node_pairs_queues = [@input, *@others].collect{|input|
	export_node_pairs = Queue.new
	Thread.start do
	  input.each_export do |*export_node_pair|
	    export_node_pairs.push export_node_pair
	  end
	  export_node_pairs.push nil
	end
	export_node_pairs
      }
      @export_node_pairs_queues_cv.broadcast
    end

    def each_export(&block)
      @export_node_pairs_queues_mutex.synchronize do
	while !@export_node_pairs_queues
	  @export_node_pairs_queues_cv.wait(@export_node_pairs_queues_mutex)
	end
      end

      for export_node_pairs in @export_node_pairs_queues
	while pair = export_node_pairs.pop
	  block.call *pair
	end
      end
    end
  end
end
