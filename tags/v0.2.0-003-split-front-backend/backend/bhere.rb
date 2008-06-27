
require "backend/b-filter"
require "backend/b-inputtable"

require "node/nhere"
require "node/port"

module Fairy
  class BHere<BFilter
    include BInputtable

    def initialize(controller)
      
      @imports = Queue.new
      super
    end

    def create_nodes
      super

      @imports.push nil
    end

    def create_and_add_node(export)
      node = super(export)

      import = Import.new
      @imports.push import
      node.export.output = import
      import.no_import = 1
    end

    def node_class
      NHere
    end

    def create_node
      node_class.new(self)
    end

    def each(&block)
      while import = @imports.pop
	import.each do |e|
	  block.call e
	end
      end
    end
  end
end
