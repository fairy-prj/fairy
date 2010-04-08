# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/n-single-exportable"

module Fairy
  class NIdentity<NSingleExportFilter
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
