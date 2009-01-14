
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BJoin<BFilter
    Controller.def_export self

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, REF)")

    def initialize(controller, opts, others, block_source)
      super
      @others = others
      @block_source = block_source
    end

    def node_class_name
      "NJoin"
    end

    def njob_creation_params
      [@block_source]
    end

    def start_create_nodes
      @other_export_queues = @others.collect{|other|
	exports = Queue.new
	Thread.start do
	  other.each_export do |export, node|
	    exports.push export
	  end
	end
	exports
      }
      super
    end

    def create_and_add_node(export, node)
      node = super
      others = @other_export_queues.collect{|queue| queue.pop	}
      node.join_inputs = others
      others.zip(node.join_imports){|other, import| other.output = import}
    end

    def break_running
      super
      @others.each{|others| Thread.start{others.break_running}}
    end
  end
end
