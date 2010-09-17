# encoding: UTF-8

require "delegate"

require "fairy/backend/bjob"
require "fairy/backend/b-filter"


module Fairy
  class BShuffle<BFilter
    Controller.def_export self

#    include BInputtable

    def initialize(controller, opts, block_source)
      super
#      @block = @context.create_proc(@block_source.source)

      @node2input = {}
      @input2node = {}
      @input_queue = PortQueue.new
      @output_queue = PortQueue.new

      @block_source = block_source
      @begin_block_source = nil
      if @opts[:BEGIN]
	@begin_block_source = @opts[:BEGIN]
      end
      @end_block_source = nil
      if @opts[:END]
	@end_block_source = @opts[:END]
      end

    end

    def node_class_name
      "NIdentity"
    end

    def njob_creation_params
      []
    end

    def each_assigned_filter(&block)
      each_node do |node|
	@input_queue.push node
      end
      @input_queue.push nil

      if @begin_block_source
	bsource = BScript.new(@begin_block_source, @context, self)
	bsource.evaluate
      end
      @block = BBlock.new(@block_source, @context, self)
      begin
	@block.call(@input_queue, @output_queue)
	@output_queue.push nil
      ensure
	if @end_block_source
	  bsource = BSource.new(@end_block_source, @context, self)
	  bsource.evaluate
	end
      end

      no = 0
      @output_queue.each do |node|
	node.no = no
	no += 1
	block.call node
      end
    end

    class PortQueue<DelegateClass(Queue)
      include Enumerable

      def initialize
	super(Queue.new)
      end

      def each
	while e = pop
	  yield e
	end
      end
    end
  end
end
