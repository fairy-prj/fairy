# encoding: UTF-8

require "fairy/backend/bjob"
require "fairy/backend/b-inputtable"

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
