require "job/filter"

module Fairy
  class Here<Filter
    def initialize(fairy)
      super
    end

    def backend_class
      BHere
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

  class BHere;end
end
