
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BGroupBy<BFilter
    Controller.def_export self

    include BInputtable

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source

      @no_of_exports = 0
      @exports = {}
      @exports_mutex = Mutex.new
      @exports_cv = ConditionVariable.new

      @exports_queue = Queue.new
    end

    def start_create_nodes
      super

      start_watch_all_node_imported
    end

    def each_export(&block)
      while pair = @exports_queue.pop
	block.call pair
      end
    end

    def add_exports(key, export, njob)
      @exports_mutex.synchronize do
	export.no = @no_of_exports
	@no_of_exports += 1
	if exports = @exports[key]
	  export.output=exports.first.output
	else
	  @exports[key] = [export]
	  @exports_queue.push [export, njob]
	end
      end
    end

    def update_exports(key, export, njob)
      add_exports(key, export, njob)
    end

    def node_class_name
      "NGroupBy"
    end

    def create_node(processor)
      processor.create_njob(node_class_name, self, @opts, @block_source)
    end

    def start_watch_all_node_imported
      Thread.start do
	while !all_node_imported?
	  @nodes_status_mutex.synchronize do
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	@exports_queue.push nil
	for key, exports in @exports
	  exports.first.output_no_import = exports.size
	end
      end
    end

    def all_node_imported?
      return false unless @number_of_nodes

      all_imported = true
      each_node(:exist_only) do |node|
	st = @nodes_status[node]
	all_imported &= [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
      end
      all_imported
    end
  end
end
