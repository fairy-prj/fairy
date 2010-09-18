# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-filter"
require "fairy/master/c-inputtable"

module Fairy
  class CIOFilter<CFilter
    include CInputtable

    def node_class
      ERR::Raise ERR::INTERNAL::UndefinedNodeClass
    end

     def input=(input)
       input.output = @input
       super
     end
    attr_reader :input

    def output=(output)
      @output = output
    end
  end
end

