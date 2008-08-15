
require "job/filter"

module Fairy
  class EachSubStreamMapper<Filter
    def initialize(fairy, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BEachSubStreamMapper"
    end
  end
end
