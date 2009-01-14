
require "backend/b-filter"
require "backend/b-inputtable"
require "backend/b-group-by"

module Fairy
  class BModGroupBy<BGroupBy
    Controller.def_export self

    def node_class_name
      "NModGroupBy"
    end
  end

  class BPreAfterModFilter<BFilter
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source
    end

    def node_class_name
      "NPreAfterModFilter"
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