
require "backend/bjob"

module Fairy
  class BFilter<BJob

    def node_class
      raise "Node Class���������Ƥ��ޤ���"
    end

    def input=(input)
      @input = input
      input.output = @input
    end

    attr_reader :input


    def output=(output)
      @output = output
    end
  end
end
