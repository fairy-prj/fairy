# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"
require "fairy/master/c-basic-group-by"

module Fairy

  module CSort
    class CPreSort<CBasicGroupBy
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super

	@samplings = []

	@pvs = nil
	@pvs_mutex = Mutex.new
	@pvs_cv = XThread::ConditionVariable.new
      end
      
      def get_pvs(buf=nil)
# BUG#271対応. 全てのセグメントからサンプルを取るのではなく, 最初のセ
# グメントからのみサンプリングを取るようにした.
	if buf
	  @samplings.push buf

# BUG#271対応. 
#	if @samplings.size >= number_of_nodes
#	    make_pvs
#	end
	  make_pvs
	end

	@pvs_mutex.synchronize do
	  while !@pvs
	    @pvs_cv.wait(@pvs_mutex)
	  end
	end
	@pvs
      end
      DeepConnect.def_method_spec(self, "DVAL get_pvs(DVAL)")

      def make_pvs
	no_segment = @opts[:no_segment]
	no_segment ||= Fairy::CONF.SORT_NO_SEGMENT


	cmp_opt = @opts[:cmp_optimize]
	cmp_opt = CONF.SORT_CMP_OPTIMIZE if cmp_opt.nil?
	
	if cmp_opt
	  key_proc = eval("proc{#{@block_source.source}}", @context.binding)
	else
	  key_proc = BBlock.new(@block_source, @context, self)
	end

	sorted = @samplings.flatten(1).map{|e| key_proc.call(e)}.sort_by{|e| e}

#Log::debugf(self, "%s", sorted.inspect)
	idxes = (1...no_segment).collect{|i| (sorted.size*i).div(no_segment)}
	@pvs_mutex.synchronize do
	  @pvs = sorted.values_at(*idxes)
	  sorted.clear
	  sorted = nil
	  @samplings.clear
	  @samplings = nil
	  @pvs_cv.broadcast
	end
      end

      def node_class_name
	"PSort::PPreSort"
      end

      def njob_creation_params
	[@block_source]
      end
    end

    class CPostSort<CIOFilter
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"PSort::PPostSort"
      end

      def njob_creation_params
	[@block_source]
      end

      def create_import(processor)
	policy = @opts[:postfilter_prequeuing_policy]
	policy ||= @opts[:prequeuing_policy]
	
	processor.create_import(policy)
      end

    end

  end
end

