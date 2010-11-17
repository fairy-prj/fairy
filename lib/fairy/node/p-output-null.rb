# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"

module Fairy
  class POutputNull<PIOFilter
    Processor.def_export self
    
    ST_OUTPUT_FINISH = :ST_OUTPUT_FINISH

#    DeepConnect.def_single_method_spec(self, "REF new(REF, REF, VAL, REF)")

    def initialize(id, ntask, bjob, opt)
      super
    end

    def input=(input)
      super
      start
    end

    def basic_start(&block)
      for l in @input
	l
      end
      self.status = ST_OUTPUT_FINISH
    end
  end
end
