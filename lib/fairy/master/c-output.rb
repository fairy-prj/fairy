# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-filter"
require "fairy/master/c-inputtable"

module Fairy
  class COutput<CFilter
    include CInputtable

    def input=(input)
      @input = input
      input.output = @input
      super
    end

  end
end
