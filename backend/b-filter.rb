
require "backend/bjob"
require "backend/b-inputtable"

module Fairy
  class BFilter<BJob
    include BInputtable

    def node_class
      raise "Node Classが定義されていません"
    end

     def input=(input)
       input.output = @input
       super
     end
#    attr_reader :input

    def output=(output)
      @output = output
    end
  end
end

