# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/port"
require "fairy/node/n-single-exportable"

module Fairy
  class NExec<NSingleExportInput
    Processor.def_export self

    def initialize(processor, bjob, opts=nil)
      super
    end

    def start(node_spec)
      super() do
	@export.push node_spec
      end
    end
  end
end
