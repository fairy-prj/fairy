
require "backend/b-filter"
require "backend/b-inputtable"
require "backend/b-group-by"

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
