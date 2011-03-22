# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"
require "fairy/master/c-basic-group-by"

module Fairy
  class CGroupBy<CBasicGroupBy
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super

      @hash_seed = controller.hash_seed
    end

    attr_reader :hash_seed

    def node_class_name
      "PGroupBy"
    end

    class CPostFilter<CIOFilter
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"PGroupBy::PPostFilter"
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

#   class BPostAfterModFilter<BFilter
#     Controller.def_export self

#     def initialize(controller, opts, block_source)
#       super
#       @block_source = block_source
#     end

#     def node_class_name
#       "NPostAfterModFilter"
#     end
#   end

  end

  class CXGroupBy<CGroupBy
    Controller.def_export self
    def node_class_name
      "PXGroupBy"
    end
  end

end
