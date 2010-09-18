# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-filter"
require "fairy/node/p-single-exportable"

module Fairy
  class PExec<PSingleExportInput
    Processor.def_export self

    def initialize(id, ntask, bjob, opts=nil)
      super
    end

    def start(nfileplace)
      @nfileplace = nfileplace
      self.no = nfileplace.no
      self
    end
    alias open start
    DeepConnect::def_method_spec(self, "REF start(VAL)")
    DeepConnect::def_method_spec(self, "REF open(VAL)")

    def basic_each(&block)
      block.call @nfileplace.url
    end
  end
end
