require "node/nhere"

module Fairy
  class BHere<BJob
    def initialize(controller)
      super
    end

    def input=(input)
      @input = input

      self.nodes=input.nodes.collect{|input_node| 
	node = NHere.new
	node.input= input_node
	node
      }
    end

    def each(&block)
      for node in nodes
	while (e = node.pop) != NHere::END_OF_STREAM
	  block.call e
	end
      end
    end
  end
end
