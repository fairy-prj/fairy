
require "delegate"

require "backend/bjob"

module Fairy
  class BShuffle<BJob
    include BInputtable

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source
      @block = @context.create_proc(@block_source)

      @input2node = {}
      @input_queue = PortQueue.new
      @output_queue = PortQueue.new
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
	begin
	  @block.call(@input_queue, @output_queue)
	  @output_queue.push nil
	rescue
	  p $!, $@
	  raise 
	end
      end
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
