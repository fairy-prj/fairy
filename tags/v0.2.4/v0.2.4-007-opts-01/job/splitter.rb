
require "job/filter"

module Fairy
  class Splitter<Filter
    def initialize(fairy, n, opts=nil)
      super
      @no_split = n
      @opts = opts
    end

    def backend_class_name
      "BSplitter"
    end
  end
end
