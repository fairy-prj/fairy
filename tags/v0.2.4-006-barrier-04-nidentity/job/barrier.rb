
require "job/filter"

module Fairy
  class Barrier<Filter
    def initialize(fairy, opts)
      super
      @opts = opts
    end

    def backend_class_name
      "BBarrier"
    end
  end
end
