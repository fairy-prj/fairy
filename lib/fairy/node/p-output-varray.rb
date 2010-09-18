# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"

require "fairy/share/varray"

module Fairy
  class POutputVArray<PIOFilter
    Processor.def_export self
    
    ST_OUTPUT_FINISH = :ST_OUTPUT_FINISH

#    DeepConnect.def_single_method_spec(self, "REF new(REF, REF, VAL, REF)")

#    def initialize(processor, bjob, opt, varray, idx)
    def initialize(id, ntask, bjob, opt, idx)
      super
      @no_in_bjob = idx
    end

    def input=(input)
      super
      start
    end


    def basic_start(&block)
      array = []
      for l in @input
	array.push l
      end
	
      processor.register_varray_element(array)
      @bjob.varray.arrays_put(@no_in_bjob, array)
      self.status = ST_OUTPUT_FINISH
    end
  end
end
