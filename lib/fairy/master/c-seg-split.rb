# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"

module Fairy
  class CSegSplit<CIOFilter
    Controller.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(controller, opts, n)
      super
      @no_split = n

      @no_of_exports = 0
      @no_of_exports_mutex = Mutex.new

#      @exports = []
#      @exports_mutex = Mutex.new
#      @exports_cv = XThread::ConditionVariable.new

#      @export2njob = {}

#      @exports_queue = Queue.new
    end

#     def start_create_nodes
#       super
#       Thread.start{appear_njob}
#     end

#     def appear_njob
# Log::debug(self, "APPEAR_NJOB:S")
#       loop do
# 	@nodes_mutex.synchronize do
# 	  while @nodes_for_next_filters.empty?
# 	    @nodes_cv.wait(@nodes_mutex)
# 	  end
# Log::debug(self, "APPEAR_NJOB:1")
# 	  njob = @nodes_for_next_filters.shift
# Log::debug(self, "APPEAR_NJOB:2")
# 	  unless njob
# Log::debug(self, "APPEAR_NJOB:E")
# 	    @exports_queue.push nil
# 	    return
# 	  end

# Log::debug(self, "APPEAR_NJOB:3")
# 	  njob.exports.each do |exp|
# 	    exp.no = @no_of_exports
# 	    #	  exp.key = njob.key
# 	    @no_of_exports += 1

# 	    @export2njob[exp] = njob
# 	    @exports_queue.push exp
# 	  end
# Log::debug(self, "APPEAR_NJOB:4")
# 	end
#       end
#     end

#     def next_filter(mapper)
#       @exports_queue.pop
#     end

#     def start_export(njob)
# Log::debug(self, "START_EXPORT:SS")

# Log::debug(self, "START_EXPORT: #{exp.class} #{@export2njob[exp].class}")
#       njob.start_export
# Log::debug(self, "START_EXPORT:EE")
#       exp
#     end

    def each_export_by(njob, mapper, &block)
      njob.exports.each do |exp|
	@no_of_exports_mutex.synchronize do
	  exp.no = @no_of_exports
	  #  exp.key = njob.key
	  @no_of_exports += 1
	end
	
	block.call exp
      end
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
      "PSegSplit"
    end
    
    def njob_creation_params
      [@no_split]
    end
  end
end
