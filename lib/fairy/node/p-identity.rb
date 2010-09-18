# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
require "fairy/node/n-single-exportable"

module Fairy
  class PIdentity<PSingleExportFilter
    Processor.def_export self

    def initialize(id, ntask, bjob, opts=nil)
      super
    end

    def basic_each(&block)
      @input.each do |e|
	block.call e
      end
    end
  end

end
