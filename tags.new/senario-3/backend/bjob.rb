
require "front/reference"

module Fairy
  class BJob
    def initialize(controller)
      @controller = controller
      @nodes = Reference.new
    end

    def nodes
      @nodes.value
    end

    def nodes=(val)
      @nodes.value = val
    end
  end
end
