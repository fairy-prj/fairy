
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

    def node_class_name
      "NZipper"
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
 puts "REATE_AND_ADD_NODE0: #{@other_export_queues.inspect}"
	others = @other_export_queues.collect{|queue| queue.pop}
 puts "REATE_AND_ADD_NODE1: #{others.inspect}"
	node.zip_inputs = others
	others.zip(node.zip_imports.to_a){|other, import| other.output = import}
      else
	raise "まだできていません"
      end
    end

    def create_node(processor)
      processor.create_njob(node_class_name, self, @opts, @block_source)
    end
  end
end
