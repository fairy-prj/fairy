
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
puts "start_create_nodes: #{export.inspect}"
puts "start_create_nodes: #{node.inspect}"
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
 puts "CREATE_AND_ADD_NODE0: #{@other_export_queues.inspect}"

#require "share/tr"

	others = @other_export_queues.collect{|queue| 
	  puts "CREATE_AND_ADD_NODE*:#{queue.inspect}"
	  queue.pop
	}
 puts "CREATE_AND_ADD_NODE1: #{others.peer_inspect}"
	node.zip_inputs = others
	others.zip(node.zip_imports.to_a){|other, import| other.output = import}
      else
	raise "�ޤ��Ǥ��Ƥ��ޤ���"
      end
    end

    def create_node(processor)
      processor.create_njob(node_class_name, self, @opts, @block_source)
    end
  end
end