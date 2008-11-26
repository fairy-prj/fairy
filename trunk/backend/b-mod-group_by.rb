
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

    def node_class_name
      "NPreAfterModFilter"
    end
  end

  class BPostAfterModFilter<BFilter
    Controller.def_export self

    def node_class_name
      "NPostAfterModFilter"
    end
  end

end
