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
  end

  class BHere;end
end
