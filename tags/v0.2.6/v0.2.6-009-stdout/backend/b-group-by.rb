
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BGroupBy<BFilter
    Controller.def_export self

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
#puts "XXXX:"
#puts @exports.keys.inspect
	if exports = @exports[key]
#	  sleep 0.1
#puts "X: #{exports.first.output.class}"

	  export.output=exports.first.output
	  exports.push export
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
	for key, exports in @exports
	  exports.first.output_no_import = exports.size
	end
      end
      nil
    end

    def all_node_imported?
      # すべてのnjobがそろったか?
      return false unless @nodes_mutex.synchronize{@number_of_nodes}

      each_node(:exist_only) do |node|
	st = @nodes_status[node]
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
