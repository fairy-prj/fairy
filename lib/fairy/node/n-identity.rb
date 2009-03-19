# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/n-single-exportable"

module Fairy
  class NIdentity<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts=nil)
      super
    end

    def start
      super do
	@import.each{|e| @export.push e}
      end
    end
  end

end
