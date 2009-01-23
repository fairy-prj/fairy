
require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BZipper<BFilter
    Controller.def_export self

    ZIP_BY_SUBSTREAM = :ZIP_BY_SUBSTREAM

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, VAL)")

    def initialize(controller, opts, others, block_source)
      super
      @others = others
      @block_source = block_source
    end

    def opt_zip_by_substream?
      @opts[ZIP_BY_SUBSTREAM]
    end

    def node_class_name
      "NZipper"
    end

    def njob_creation_params
      [@block_source]
    end

    def start_create_nodes
      @other_export_queues = @others.collect{|other|
	exports = Queue.new
	Thread.start do
	  other.each_export do |export, node|
	    exports.push export
	  end
	end
	exports
      }
      super
    end

    def create_and_add_node(export, node)
      node = super
      if opt_zip_by_substream?
	others = @other_export_queues.collect{|queue| queue.pop	}
	node.zip_inputs = others
	others.zip(node.zip_imports){|other, import| other.output = import}
      else
	raise "まだできていません"
      end
    end
  end
end
