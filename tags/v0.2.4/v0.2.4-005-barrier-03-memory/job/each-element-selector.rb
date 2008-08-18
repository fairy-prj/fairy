
require "job/filter"

module Fairy
  class EachElementSelector<Filter
    def initialize(fairy, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BEachElementSelector"
    end
  end

end
