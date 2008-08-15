
require "backend/b-filter"
require "backend/b-inputtable"

require "node/port"


module Fairy
  class BHere<BFilter
    Controller.def_export self

    include BInputtable

    def initialize(controller, opts=nil)
      
      @imports = Queue.new
      super
    end

    def create_nodes
      super

      @imports.push nil
    end

    def create_and_add_node(export, bjob)
      node = super(export, bjob)
      import = Import.new
      @imports.push import
      node.export.output = import
      import.no_import = 1
    end

    def node_class_name
      "NHere"
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


