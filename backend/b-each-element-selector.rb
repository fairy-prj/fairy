# encoding: UTF-8

require "backend/b-filter"
require "backend/b-inputtable"

module Fairy
  class BEachElementSelector<BFilter
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source
    end

    def node_class_name
      "NEachElementSelector"
    end

    def njob_creation_params
      [@block_source]
    end

  end
end