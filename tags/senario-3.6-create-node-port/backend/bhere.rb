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
      create_nodes
    end

    def create_nodes
      Thread.start do
	no = 0
	imports = []
	@input.each_export do |export|
	  node = NHere.new(self)
	  node.input= export
	  export.output = node.import
	  add_node node
	  no += 1

	  import = Import.new
	  imports.push import
	  node.output = import
	end
	self.number_of_nodes = no
	@imports.value = imports
      end
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
