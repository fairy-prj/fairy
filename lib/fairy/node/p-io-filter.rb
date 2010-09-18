# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-filter"

module Fairy
  class PIOFilter<PFilter
    Processor.def_export self

    ST_WAIT_IMPORT = :ST_WAIT_IMPORT

    def initialize(id, ntask, bjob, opts=nil, *rests)
      super
      self.status = ST_WAIT_IMPORT
    end

    def input=(input)
      @input = input
      if input.kind_of?(Import)
	input.njob_id = @id
      end
      self.no = input.no
      self.key = input.key
    end
  end
end
