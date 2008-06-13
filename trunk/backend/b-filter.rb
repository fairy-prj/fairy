
require "backend/bjob"
require "node/n-filter"

module Fairy
  class BFilter<BJob

    def node_class
      raise "Node Classが定義されていません"
    end

    def input=(input)
      @input = input
      input.output = @input
    end


    def output=(output)
      @output = output
    end
  end
end

