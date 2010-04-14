
require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"
require "fairy/backend/b-group-by"

module Fairy
  class BModGroupBy<BGroupBy
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super

      @hash_seed = controller.hash_seed
    end

    attr_reader :hash_seed

    def node_class_name
      "NModGroupBy"
    end

    class BPostFilter<BFilter
      Controller.def_export self

      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"NModGroupBy::NPostFilter"
      end

      def njob_creation_params
	[@block_source]
      end

      def create_import(processor)
	policy = @opts[:postfiler_prequeuing_policy]
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
end
