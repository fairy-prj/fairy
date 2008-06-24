
require "node/n-filter"

module Fairy
  class NFilter1to1<NFilter
    def initialize
      super
      @export = Export.new
    end

    def output=(output)
      @export.output= output.import
    end

#     def pop
#       @export.pop
#     end
  end
end
