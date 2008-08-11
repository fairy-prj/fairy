require "job/filter"

module Fairy
  class Here<Filter
    include Enumerable

    def initialize(fairy)
      super
    end

    def backend_class_name
      "BHere"
    end

    def each(&block)
      backend.each{|e| block.call e}
    end

    def to_a
      ary = []
      backend.each{|e| ary.push e}
      ary
    end

  end

#  class BHere;end
end
