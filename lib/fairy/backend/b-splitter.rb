# encoding: UTF-8

require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"

module Fairy
  class BSplitter<BFilter
    Controller.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(controller, opts, n)
      super
      @no_split = n

      @no_of_exports = 0

#      @exports = []
#      @exports_mutex = Mutex.new
#      @exports_cv = ConditionVariable.new

      @export2njob = {}

      @exports_queue = Queue.new
    end

    def start_create_nodes
      super
      Thread.start{appear_njob}
    end

    def appear_njob
      @nodes_mutex.synchronize do
	while @nodes_for_next_filters.empty?
	  @nodes_cv.wait(@nodes_mutex)
	end
	njob = @nodes_for_next_filters.shift
	unless njob
	  @exports.push nil
	  return
	end

	njob.exports.each do |exp|
	  exp.no = @no_of_exports
#	  exp.key = njob.key
	  @no_of_exports += 1

	  @export2njob[exp] = njob
	  @exports_queue.push exp
	end
      end
    end

    def next_filter(mapper)
      @exports_queue.pop
    end

    def start_export(exp)
      @export2njob[exp].start_export
      exp
    end
      

#     def each_export(&block)
#       each_node do |node|
# 	for exp in node.exports
# 	  exp.no = @no_of_exports
# 	  @no_of_exports += 1
# 	  block.call exp, node
# 	  exp.output_no_import = 1
# 	end
#       end
#     end

    def node_class_name
      "NSplitter"
    end
    
    def njob_creation_params
      [@no_split]
    end
  end
end
