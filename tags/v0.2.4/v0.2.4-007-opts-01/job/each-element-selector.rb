
require "job/filter"

module Fairy
  class EachElementSelector<Filter
    def initialize(fairy, block_source, opts=nil)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "BEachElementSelector"
    end
  end

end
