# encoding: UTF-8

require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"

module Fairy
  class BGroupBy<BFilter
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source

      @no_of_exports = 0

      # key -> [export, ...]
      @exports = {}
      @exports_mutex = Mutex.new
      @exports_cv = ConditionVariable.new

      @exports_queue = Queue.new

      @each_export_by_thread = nil
      @each_export_by_thread_mutex = Mutex.new
    end

    def start_create_nodes
      super

      start_watch_all_node_imported
    end

#    def each_export(&block)
#      while pair = @exports_queue.pop
#	block.call pair
#      end
#    end

    def next_filter(mapper)
      ret = super
      unless ret
	@each_export_by_thread_mutex.synchronize do
	  @each_export_by_thread.join if @each_export_by_thread
	end
      end
      ret 
    end

#     def each_export_by(njob, mapper, &block)
#       return if @each_export_by_thread

#       begin
# 	while pair = @exports_queue.pop
# 	  exp, njob = pair
# 	  Log::debug(self, "EXPORT_BY, #{exp.key}")
# 	  block.call exp
# 	end
#       rescue
# 	Log::fatal_exception
#       end
#       @each_export_by_thread = true
#     end

    def each_export_by(njob, mapper, &block)
      @each_export_by_thread_mutex.synchronize do
	return if @each_export_by_thread

	@each_export_by_thread = Thread.start{
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

    def bind_export(exp, imp)
      # do nothing
    end

    #
    #
    def add_exports(key, export, njob)
      @exports_mutex.synchronize do
	if exports = @exports[key]
	  export.output=exports.first.output
	  export.no = exports.first.no
	  exports.push export
	else
	  export.no = @no_of_exports
	  @no_of_exports += 1
	  @exports[key] = [export]
	  @exports_queue.push [export, njob]
	end
      end
    end

    def update_exports(key, export, njob)
      add_exports(key, export, njob)
      nil
    end

    def node_class_name
      "NGroupBy"
    end

    def njob_creation_params
      [@block_source]
    end

    def start_watch_all_node_imported
      Thread.start do
	@nodes_status_mutex.synchronize do
	  while !all_node_imported?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	@exports_queue.push nil
#Log::debug(self, "START: setting for EXPOTRS.SIZE")
	for key, exports in @exports
#Log::debug(self, "EXPOTRS.SIZE=#{exports.size}")
	  exports.first.output_no_import = exports.size
	end
#Log::debug(self, "END: setting for EXPOTRS.SIZE")
      end
      nil
    end

    def all_node_imported?
      # すべてのnjobがそろったか?
      return false unless @nodes_mutex.synchronize{@number_of_nodes}

      each_node(:exist_only) do |node|
	st = @nodes_status[node]
# こちらはNG: outputが設定されていないとまずい.
#	unless [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH, :ST_ALL_IMPORTED].include?(st)
	unless [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
	  return false
	end
      end
      true
    end
  end

  class BMGroupBy<BGroupBy
    Controller.def_export self

    def node_class_name
      "NMGroupBy"
    end
  end


end
