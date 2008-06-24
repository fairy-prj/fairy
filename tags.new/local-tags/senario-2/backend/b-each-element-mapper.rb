
module Fairy
  class BEachElementMapper
    def initialize(controller, block_source)
      @controller = controller
      @block_source = block_source
    end

    def input=(input)
      @input = input
    end
  end
end
