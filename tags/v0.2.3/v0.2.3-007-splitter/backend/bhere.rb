
require "backend/b-filter"
require "backend/b-inputtable"

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

    def create_node(processor)
      njob = processor.create_njob(node_class_name, self)
      njob
    end

    def each(&block)
      while import = @imports.pop
 puts "EACH0: #{import}"
	import.each do |e|
 puts "EACH1: #{e.inspect}"
	  block.call e
	end
      end
    end
  end
end
