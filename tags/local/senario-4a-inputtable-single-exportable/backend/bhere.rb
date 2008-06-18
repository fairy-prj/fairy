
require "backend/b-filter"
require "backend/b-inputtable"

require "node/nhere"
require "node/port"

module Fairy
  class BHere<BFilter
    include BInputtable

    def initialize(controller)
      
#      @imports = Reference.new
      @imports = Queue.new
      super
    end

#     def create_nodes
#       Thread.start do
# 	no = 0
# 	imports = []
# 	@input.each_export do |export|
# 	  node = NHere.new(self)
# 	  node.input= export
# 	  export.output = node.import
# 	  add_node node
# 	  no += 1
#
# 	  import = Import.new
# 	  imports.push import
# 	  node.export.output = import
# 	  import.no_import = 1
# 	end
# 	self.number_of_nodes = no
# 	@imports.value = imports
#       end
#     end

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
