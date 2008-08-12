
require "job/filter"

module Fairy
  class GroupBy<Filter
    def initialize(fairy, block_source, opts=nil)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "BGroupBy"
    end
  end
end
