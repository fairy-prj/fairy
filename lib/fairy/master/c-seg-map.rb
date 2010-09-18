# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"

module Fairy
  class CSegMap<CIOFilter
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source
    end

    def node_class_name
      "PSegMap"
    end

    def njob_creation_params
      [@block_source]
    end
  end
end
