# encoding: UTF-8

require "delegate"

require "fairy/backend/bjob"

module Fairy
  class BShuffle<BFilter
    Controller.def_export self

#    include BInputtable

    def initialize(controller, opts, block_source)
      super
#      @block = @context.create_proc(@block_source.source)

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

    def input=(input)
      @input = input
      start_get_exports
      start
    end

    def start_get_exports
      Thread.start do
	@input.each_export do |export, node|
	  @input2node[export] = node
	  @input_queue.push export
	end
	@input_queue.push nil
      end
    end

    def start
      Thread.start do
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
      end
      nil
    end

    def each_export(&block)
      for exp in @output_queue
	node = @input2node[exp]
	block.call exp, node
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
