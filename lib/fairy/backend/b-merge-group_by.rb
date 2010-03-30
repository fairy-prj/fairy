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

    def each_export_by(njob, mapper, &block)
      @each_export_by_thread_mutex.synchronize do
	return if @each_export_by_thread

	@each_export_by_thread = Thread.start{
	  # すべての njob がそろうまで待つ
	  # 後段が先にスケジュールされてデッドロックするのを避けるため.
	  number_of_nodes

	  begin
	    while pair = @exports_queue.pop
	      exp, njob = pair
Log::debug(self, "EXPORT_BY, #{exp.key}")
	      block.call exp
	    end
	  rescue
	    Log::fatal_exception
	    raise
	  end
	}
      end
    end


    def add_exports(key, export, njob)
      @exports_mutex.synchronize do
	export.no = @no_of_exports
	@no_of_exports += 1
	unless expexp = @exports[key]
	  policy = @opts[:postsuperqueue_queuing_policy]
	  @exports[key] = expexp = Export.new(policy)
	  expexp.no = @exports.size - 1
	  expexp.add_key key
	  @exports_queue.push [expexp, njob]
#	  @pre_exports_queue.push [expexp, njob]
	  expexp.output_no_import = 1
	end
	expexp.push_delayed_element {|context|
	  policy = @opts[:subqueue_queuing_policy]
	  imp = context.context_eval(%{Import.new(#{policy.inspect})})
	  imp.no = export.no
	  imp.add_key(key)
	  imp.set_log_callback(%q{|n| 
	    Log::verbose(self, "IMPORT POP: #{n}")
	  }, nil, __FILE__, __LINE__ - 1)

	  export.output = imp
	  export.output_no_import = 1
	  imp
	}
#	export.start_export
      end
    end

#     def add_exports(key, export, njob)
#       @exports_mutex.synchronize do
# 	export.no = @no_of_exports
# 	@no_of_exports += 1
# 	unless expexp = @exports[key]
# 	  policy = @opts[:postsuperqueue_queuing_policy]
# 	  @exports[key] = expexp = Export.new(policy)
# 	  expexp.no = @exports.size - 1
# 	  expexp.add_key key
# 	  @pre_exports_queue.push [expexp, njob]
# 	  expexp.output_no_import = 1
# 	end
# 	policy = @opts[:subqueue_queuing_policy]
# 	imp = Import.new(policy)
# 	imp.no = export.no
# 	imp.add_key(key)
# 	imp.set_log_callback do |n| 
# 	  Log::verbose(self, "IMPORT POP: #{n}")
# 	end

# 	export.output = imp
# 	expexp.push imp
# 	export.output_no_import = 1
#       end
#     end

#     def start_watch_all_node_imported
#       Thread.start do
# 	@nodes_status_mutex.synchronize do
# 	  while !all_node_imported?
# 	    @nodes_status_cv.wait(@nodes_status_mutex)
# 	  end
# 	end
# 	@exports_queue.push nil
# 	for key, exports in @exports
# 	  exports.push :END_OF_STREAM
# 	end
#       end
#       nil
#     end

    def start_watch_all_node_imported
      Thread.start do
	# すべての njob がそろうまで待つ
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: S")
	number_of_nodes

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1")

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 2")
	# すべての exports がそろうまで待つ
	@nodes_status_mutex.synchronize do
	  while !all_node_imported?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	@exports_queue.push nil
	
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 4")
	for key, exports in @exports
	  exports.push :END_OF_STREAM
	end
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: E")
      end
      nil
    end

    def start_watch_all_node_imported_ORG
      Thread.start do
	# すべての njob がそろうまで待つ
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: S")
	number_of_nodes

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1")
	# すでに存在するexportsを下流に送る
	@exports_mutex.synchronize do
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1.1")
	  @pre_exports_queue.push nil
	  while pair = @pre_exports_queue.pop
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1.L")
	    @exports_queue.push pair
	  end
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1.E")
	end

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 2")
	# すべての exports がそろうまで待つ
	@nodes_status_mutex.synchronize do
	  while !all_node_imported?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 3")
	# 残りのexportsを下流に送る
	@pre_exports_queue.push nil
	while pair = @pre_exports_queue.pop
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 3.L")
	  @exports_queue.push pair
	end
	@exports_queue.push nil
	
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 4")
	for key, exports in @exports
	  exports.push :END_OF_STREAM
	end
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: E")
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
