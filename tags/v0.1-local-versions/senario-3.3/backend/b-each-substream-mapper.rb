
require "backend/bjob"
require "node/n-each-substream-mapper"

module Fairy
  class BEachSubStreamMapper<BJob
    def initialize(controller, block_source)
      super(controller)
      @block_source = block_source
    end

    def input=(input)
      @input = input

      self.nodes = input.nodes.collect{|input_node|
	node = NEachSubStreamMapper.new(@block_source)
	node.input= input_node
	node
      }
    end
  end
end
