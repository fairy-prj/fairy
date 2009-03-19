# encoding: UTF-8

require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"
require "fairy/backend/b-group-by"

require "fairy/node/port"

module Fairy
  class BMergeGroupBy<BGroupBy
    Controller.def_export self

    def node_class_name
      "NMergeGroupBy"
    end

    def add_exports(key, export, njob)
      @exports_mutex.synchronize do
	export.no = @no_of_exports
	@no_of_exports += 1
	unless expexp = @exports[key]
	  policy = @opts[:postqueuing_policy]
	  @exports[key] = expexp = Export.new(policy)
	  expexp.no = @exports.size - 1
	  expexp.add_key key
	  @exports_queue.push [expexp, njob]
	  expexp.output_no_import = 1
	end
	policy = @opts[:subqueue_queuing_policy]
	imp = Import.new(policy)
	imp.no = export.no
	imp.add_key(key)
	export.output = imp
	expexp.push imp
	export.output_no_import = 1
      end
    end

    def start_watch_all_node_imported
      Thread.start do
	@nodes_status_mutex.synchronize do
	  while !all_node_imported?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	@exports_queue.push nil
	for key, exports in @exports
	  exports.push :END_OF_STREAM
	end
      end
      nil
    end

#     class BPostFilter<BFilter
#       Controller.def_export self

#       def initialize(controller, opts, block_source)
# 	super
# 	@block_source = block_source

# 	@input2node = {}
# 	@input_queue = PortQueue.new
# 	@output_queue = PortQueue.new
#       end
      
#       def input=(input)
# 	@input = input
# 	start_get_exports
# 	start
#       end

#       def start_get_exports
# 	Thread.start do
# 	  @input.each_export do |export, node, key|
# 	    @input2node[export] = node
# 	    @input_queue.push [export
# 	  end
# 	  @input_queue.push nil
# 	end
#       end

#       def start
# 	Thread.start do
# 	  begin
# 	    @block.call(@input_queue, @output_queue)
# 	    @output_queue.push nil
# 	  ensure
# 	    if @end_block_source
# 	      bsource = BSource.new(@end_block_source, @context, self)
# 	      bsource.evaluate
# 	    end
# 	  end
# 	end
# 	nil
#       end

#       def each_export(&block)
# 	@input.each_export do |export, node, key|
	  
# 	for exp in @output_queue
# 	  node = @input2node[exp]
# 	  block.call exp, node
# 	end
#       end
#     end

  end
end
