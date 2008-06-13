require "node/nhere"
require "node/port"

module Fairy
  class BHere<BJob
    def initialize(controller)
      super
      
      @imports = Reference.new
    end

    def input=(input)
      @input = input

      imports = []
      self.nodes=input.nodes.collect{|input_node| 
	node = NHere.new(self)
	node.input= input_node
	input_node.output = node

 	import = Import.new
 	imports.push import
 	node.output = import
	node
      }
      @imports.value = imports

    end

    def each(&block)
      @imports.value.each do |import|
	import.each do |e|
	  block.call e
	end
      end
    end
  end
end
