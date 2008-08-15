
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BSplitter<BFilter
    Controller.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(controller, opts, n)
      super
      @no_split = n

      @no_of_exports = 0

#       @exports = []
#       @exports_mutex = Mutex.new
#       @exports_cv = ConditionVariable.new

#       @exports_queue = Queue.new
    end

    def start_create_nodes
      super

    end

    def each_export(&block)
      each_node do |node|
	for exp in node.exports
	  exp.no = @no_of_exports
	  @no_of_exports += 1
	  block.call exp, node
	  exp.output_no_import = 1
	end
      end

    end

    def node_class_name
      "NSplitter"
    end
    
    def njob_creation_params
      [@no_split]
    end
  end
end
