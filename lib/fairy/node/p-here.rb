# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
require "fairy/node/p-single-exportable"

module Fairy
  class PHere<PIOFilter
    Processor.def_export self

    include PSingleExportable

    def basic_each(&block)
      @input.each do |e|
	block.call e
      end
    end
  end
end
