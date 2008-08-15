
require "backend/bjob"
require "backend/b-inputtable"

module Fairy
  class BOutput<BJob
    include BInputtable

    def input=(input)
      @input = input
      input.output = @input
      super
    end

  end
end
