# encoding: UTF-8

require "fairy/backend/bjob"
require "fairy/backend/b-inputtable"

module Fairy
  class BFilter<BJob
    include BInputtable

    def node_class
      ERR::Raise ERR::INTERNAL::UndefinedNodeClass
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

