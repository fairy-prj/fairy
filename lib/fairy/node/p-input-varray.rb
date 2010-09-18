# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-single-exportable"

module Fairy
  class PInputVArray<PSingleExportInput
    Processor.def_export self

#    def initialize(processor, bjob, opts)
#      super
#    end

    def open(nvarrayplace)
      @array = nvarrayplace.ary
      self.no = nvarrayplace.no
    end

    def basic_each(&block)
      @array.each &block
    end
  end
end

