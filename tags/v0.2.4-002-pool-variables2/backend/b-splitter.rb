
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BSplitter<BFilter
    include BInputtable

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(controller, n, opts)
      super(controller)
      @no_split = n
      @opts = opts

      @exports = []
      @exports_mutex = Mutex.new
      @exports_cv = ConditionVariable.new

      @exports_queue = Queue.new
    end

    def start_create_nodes
      super

    end

    def each_export(&block)
      each_node do |node|
	for exp in node.exports
	  block.call exp, node
	  exp.output.no_import = 1
	end
      end

    end

    def node_class_name
      "NSplitter"
    end

    def create_node(processor)
      processor.create_njob(node_class_name, self, @no_split, @opts)
    end
  end
end
