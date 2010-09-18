# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "forwardable"

require "deep-connect/deep-connect"

require "fairy/master/c-io-filter"

module Fairy
  class CDirectProduct<CIOFilter
    extend Forwardable

    Controller.def_export self

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, REF)")

    def initialize(controller, opts, others, block_source)
      super

      @others = others
      @block_source = block_source

      @main_prefilter = CPreFilter.new(@controller, @opts, block_source)
      @main_prefilter.main = self
      @other_prefilters = []
      @others.each do |other|
	prefilter = CPreFilter.new(@controller, @opts, block_source)
	prefilter.main = self
	@other_prefilters.push prefilter
      end
      @postfilter = CPostFilter.new(@controller, @opts, block_source)

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

    def each_assigned_filter(&block)
      @postfilter.each_assigned_filter &block
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

    class CPreFilter<CIOFilter
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super
	@block_source = block_source

	@no = 0
	@exports = {}
	@exports_mutex = Mutex.new
#	@exports_cv = ConditionVariable.new

	@products = nil
	@products_mutex = Mutex.new
	@products_cv = ConditionVariable.new
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

      def start_create_nodes
 	Log::debug self, "START_CREATE_NODES: #{self}"
 	@main.other_prefilters.each do |other|
 	  Thread.start do
 	    other.each_assigned_filter do |input_filter|
 	      exp = input_filter.start_export
 	    end
 	  end
 	end
	super
      end

      def each_assigned_filter(&block)
	Thread.start do
	  @main.other_prefilters.each do |p| 
	    p.each_node do |n| 
	      @exports_mutex.synchronize do 
		@exports[n] = n.exports.dc_dup
#		@exports_cv.broadcast
	      end
	    end
	  end
	  @products_mutex.synchronize do 
	    @products = nodes.product(*@main.other_prefilters.collect{|p| p.nodes})
	    @products_cv.broadcast
	  end
	end

	super
      end

      # main prefilter 用
      def each_export_by(njob, mapper, &block)
	@exports_mutex.synchronize do
	  @exports[njob] = njob.exports.dc_dup
#	  @exports_cv.broadcast
	end
	@products_mutex.synchronize do
	  while !@products
	    @products_cv.wait(@products_mutex)
	  end
	  
	  post_njob_no = -1
	  @products.each do |main_njob, *others_njobs|
	    post_njob_no += 1
	    next if main_njob != njob
	    @others_njobs = others_njobs
	    
	    block.call(@exports[main_njob].shift,
		       :init_njob => proc{|njob| 
			 njob.no = post_njob_no
			 njob.other_inputs = others_njobs.collect{|n| @exports[n].shift}})
	  end
	end
      end

      def bind_export(exp, imp)
	# do nothing
      end
    end

    class CPostFilter<CIOFilter
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"PDirectProduct::PPostFilter"
      end

      def njob_creation_params
	[@block_source]
      end
    end
  end
end
