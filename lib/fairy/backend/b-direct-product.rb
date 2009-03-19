# encoding: UTF-8

require "forwardable"

require "deep-connect/deep-connect"

require "fairy/backend/b-filter"

module Fairy
  class BDirectProduct<BFilter
    extend Forwardable

    Controller.def_export self

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, REF)")

    def initialize(controller, opts, others, block_source)
      super

      @others = others
      @block_source = block_source

      @main_prefilter = BPreFilter.new(@controller, @opts, block_source)
      @main_prefilter.main = self
      @other_prefilters = []
      @others.each do |other|
	prefilter = BPreFilter.new(@controller, @opts, block_source)
	prefilter.main = self
	@other_prefilters.push prefilter
      end
      @postfilter = BPostFilter.new(@controller, @opts, block_source)

      @prefilter_no_nodes = {}
      @prefilter_no_nodes_mutex = Mutex.new
      @prefilter_no_nodes_cv = ConditionVariable.new
    end

    attr_reader :other_prefilters

    def all_prefilters
      [@main_prefilter, *@other_prefilters]
    end

    def njob_creation_params
      [@block_source]
    end

    def each_export(&block)
      @postfilter.each_export &block
    end

    def input=(other)
      @main_prefilter.input = other
      @others.zip(@other_prefilters) do |o, prefilter|
	prefilter.input = o
      end

      @postfilter.input = @main_prefilter
    end

    def update_prefilter_no_nodes(prefilter)
      @prefilter_no_nodes_mutex.synchronize do
	@prefilter_no_nodes[prefilter] = prefilter.number_of_nodes
	@prefilter_no_nodes_cv.broadcast
      end
    end

    def no_of_exports_for_prefilter(prefilter)
      all_prefilters.reject{|f| prefilter==f}.inject(1){|dp, f|
	@prefilter_no_nodes_mutex.synchronize do
	  while (v = @prefilter_no_nodes[f]).nil?
	    @prefilter_no_nodes_cv.wait(@prefilter_no_nodes_mutex)
	  end
	  dp *= v
	end
      }
    end

# 呼ばれない
#    def start_create_nodes
#      @main_prefilter.start_create_nodes
#    end

    class BPreFilter<BFilter
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def main=(main)
	@main = main
      end

      def node_class_name
	"NDirectProduct::NPreFilter"
      end

      def njob_creation_params
	[@block_source]
      end

      def number_of_nodes=(no)
	super
	@main.update_prefilter_no_nodes(self)
      end

      def number_of_exports
	@main.no_of_exports_for_prefilter(self)
      end

      # main prefilter 用
      def each_export &block
	exports = {}
	each_node{|n| exports[n] = n.exports.dup}
	@main.other_prefilters.each{|p| p.each_node{|n| exports[n] = n.exports.dup}}
	products = nodes.product(*@main.other_prefilters.collect{|p| p.nodes})
	products.each do |main_njob, *others_njobs|

	  block.call(exports[main_njob].shift, 
		     main_njob, 
		     :init_njob => proc{|njob| njob.other_inputs = others_njobs.collect{|n| exports[n].shift}})
	  # othersのno_importの指定は, njob側でしている.
	  #main_njob.export.output_no_import = 1
	end
      end
    end

    class BPostFilter<BFilter
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"NDirectProduct::NPostFilter"
      end

      def njob_creation_params
	[@block_source]
      end
    end
  end
end
