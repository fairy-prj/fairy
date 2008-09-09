require "forwardable"

require "deep-connect/deep-connect"

require "backend/b-filter"

module Fairy
  class BInject<BFilter
    extend Forwardable

    Controller.def_export self

    def initialize(controller, opts, block_source)
      super

      @block_source = block_source

      @blocal_inject = BLocalInject.new(controller, opts, block_source)
      @bwide_inject = BWideInject.new(controller, opts, block_source)

    end

    def_delegator(:@bwide_inject, :value)

    def input=(input)
      @blocal_inject.input = input
      @bwide_inject.input = @blocal_inject
    end

    class BLocalInject<BFilter
      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"NLocalInject"
      end

      def njob_creation_params
	[@block_source]
      end
      
      def each_export(&block)

	no = 0
	first_node = nil
	each_node do |node|
	  no += 1
	  if first_node
	    node.export.output = first_node.export.output
	  else
	    first_node = node
	    block.call node.export, node
	  end
	end
	first_node.export.output_no_import = no
      end

    end

    class BWideInject<BFilter
      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"NWideInject"
      end

      def njob_creation_params
	[@block_source]
      end

      def value
	each_node{|node| return node.value}
      end
    end
  end
end
