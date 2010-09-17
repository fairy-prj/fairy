# encoding: UTF-8

require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"

module Fairy
  class BEachSubStreamMapper<BFilter
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source
    end

    def node_class_name
      "NEachSubStreamMapper"
    end

    def njob_creation_params
      [@block_source]
    end
  end
end