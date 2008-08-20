
require "job/filter"

module Fairy
  class GroupBy<Filter
    def initialize(fairy, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BGroupBy"
    end
  end
end