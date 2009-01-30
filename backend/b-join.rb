
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BJoin<BFilter
    Controller.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, REF)")

    def initialize(controller, opts, others, block_source)
      super
      @others = others
      @block_source = block_source
    end

    def join_by
      by = @opts[:by]
      return :ORDER unless by
      by.to_s.upcase.intern
    end

    def node_class_name
      "NJoin"
    end

    def njob_creation_params
      [@block_source]
    end

    def start_create_nodes
      case join_by
      when :ORDER
	start_create_nodes_by_order
      when :KEY
	start_create_nodes_by_key
      else
	raise "そのオプションは分かりません"
      end
      super
    end

    def create_and_add_node(export, node)
      node = super
      case join_by
      when :ORDER
	create_and_add_node_by_order(export, node)
      when :KEY
	create_and_add_node_by_key(export, node)
      end
    end

    # by order
    def start_create_nodes_by_order
      @other_export_queues = @others.collect{|other|
	exports = Queue.new
	Thread.start do
	  other.each_export do |export, node|
	    exports.push export
	  end
	end
	exports
      }
    end

    def create_and_add_node_by_order(export, node)
      others = @other_export_queues.collect{|queue| queue.pop	}
      node.join_inputs = others
      others.zip(node.join_imports){|other, import| other.output = import}
    end

    # by key
    def start_create_nodes_by_key
      @other_exports = nil
      @other_exports_mutex = Mutex.new
      @other_exports_cv = ConditionVariable.new

      Thread.start do
	@other_exports_mutex.synchronize do
	  @other_exports = @others.collect{|other|
	    exports = {}
	    other.each_export do |export, node|
	      exports[export.key] = export
	    end
	    exports
	  }
	  @other_exports
	end
      end
    end

    def create_and_add_node_by_key(export, node)
      @other_exports_mutex.synchronize do
	while !@other_exports
	  @other_exports_cv.wait(@other_exports_mutex)
	end
      end

      others = @other_exports.collect{|exports| exports[export.key]}
      node.join_inputs = others
      others.zip(node.join_imports){|other, import| other.output = import if other}
    end

    def break_running
      super
      @others.each{|others| Thread.start{others.break_running}}
    end
  end
end
