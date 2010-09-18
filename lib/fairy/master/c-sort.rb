# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

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
	@pvs_cv = ConditionVariable.new
      end
      
      def get_pvs(buf)
	@samplings.push buf

	if @samplings.size >= number_of_nodes
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
	n_group_by = @opts[:n_group_by]
	n_group_by ||= Fairy::CONF.SORT_N_GROUP_BY


	hash_opt = @opts[:hash_optimize]
	hash_opt = CONF.HASH_OPTIMIZE if hash_opt.nil?
	
	if hash_opt
	  key_proc = eval("proc{#{@block_source.source}}", @context.binding)
	else
	  key_proc = BBlock.new(@block_source, @context, self)
	end


	sorted = @samplings.flatten.sort_by{|e| key_proc.call(e)}
#Log::debug(self, "%s", sorted.inspect)
	idxes = (1...n_group_by).collect{|i| (sorted.size*i).div(n_group_by)}
	@pvs_mutex.synchronize do
	  @pvs = sorted.values_at(*idxes)
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

