require "forwardable"

require "deep-connect/deep-connect"

require "backend/b-filter"

module Fairy
  class BFind<BFilter
    extend Forwardable

    Controller.def_export self

    def initialize(controller, opts, block_source)
      super

      @block_source = block_source

      @blocal_find = BLocalFind.new(controller, opts, block_source)
      @bfind_result = BFindResult.new(controller, opts, self)

    end

    def_delegator :@bfind_result, :value
    def_delegator :@bfind_result, :output=
    def_delegator :@bfind_result, :each_export

    def input=(input)
      @blocal_find.input = input
      @bfind_result.input = @blocal_find
    end

    def update_find
      @blocal_find.find_break
    end

    class BLocalFind<BFilter
      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"NLocalFind"
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

      def find_break
	# create node 中ならそれをとめる
	break_create_node
	# 各tasklettをとめる
	each_node do |tasklet|
	  tasklet.find_break
	end
      end

    end

    class BFindResult<BFilter
      def initialize(controller, opts, bfind)
	super
	@bfind = bfind

	@find_mutex = Mutex.new
	@findp = false
      end

      def node_class_name
	"NFindResult"
      end

      def njob_creation_params
	[]
      end

      def value
	each_node{|node| return node.value}
      end

      def update_find
	@find_mutex.synchronize do
	  if !@findp
	    @findp = true
	    @bfind.update_find
	  end
	end
      end
    end
  end
end
