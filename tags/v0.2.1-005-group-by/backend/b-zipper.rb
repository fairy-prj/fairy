
require "backend/b-filter"
require "backend/b-inputtable"

require "node/n-zipper"

module Fairy
  class BZipper<BFilter
    include BInputtable

    ZIP_BY_SUBSTREAM = :ZIP_BY_SUBSTREAM

    def initialize(controller, opts, others, block_source)
      super(controller)
      @opts = opts.to_a
      @others = others.to_a
      @block_source = block_source
    end

    def opt_zip_by_substream?
      @opts.include?(ZIP_BY_SUBSTREAM)
    end

    def node_class
      NZipper
    end

    def start_create_nodes
      @others_exports_queue = @others.collect{|other|
	exports = Queue.new
	Thread.start do
	  other.each_export do |export|
	    exports.push export
	  end
	end
	exports
      }
      super
    end

    def create_and_add_node(export)
      node = super
      if opt_zip_by_substream?
	others = @others_exports_queue.collect{|queue| queue.pop}
	node.zip_inputs = others
	others.zip(node.zip_imports){|other, import| other.output = import}
      end
    end

    def create_node
      node_class.new(self, @opts, @block_source)
    end
  end
end
