
require "backend/b-filter"
require "node/n-group-by"

module Fairy
  class BGroupBy<BFilter
    def initialize(controller, block_source)
      super(controller)
      @block_source = block_source

      @exports = {}
      @exports_mutex = Mutex.new
      @exports_cv = ConditionVariable.new

      @exports_queue = Queue.new
    end

    def input=(input)
      super
      create_nodes
    end

    def create_nodes
      Thread.start do
	no = 0
	@input.each_export do |export|
	  node = create_node
	  node.input= export
	  export.output = node.import
	  add_node node
	  no += 1
	end
	self.number_of_nodes = no
      end

      start_watch_all_node_imported
    end

    def add_exports(key, export)
      @exports_mutex.synchronize do
	if exports = @exports[key]
	  export.output=exports.first.output
	else
	  @exports[key] = [export]
	  @exports_queue.push export
	end
      end
    end

    def each_export(&block)
      while export = @exports_queue.pop
	block.call export
      end

      for key, exports in @exports
	exports.first.output.no_import = exports.size
      end

    end

    def update_exports(key, export)
      add_exports(key, export)
    end

    def node_class
      NGroupBy
    end

    def create_node
      node_class.new(self, @block_source)
    end

    def start_watch_all_node_imported
      Thread.start do
	while !all_node_imported?
	  @nodes_status_mutex.synchronize do
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	@exports_queue.push nil
      end
    end

     def all_node_imported?
      return false unless number_of_nodes

      all_imported = true
      each_node(:exist_only) do |node|
	st = @nodes_status[node]
	all_imported &= [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
      end
      all_imported
    end
  end
end
