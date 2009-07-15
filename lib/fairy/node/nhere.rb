# encoding: UTF-8

require "fairy/node/n-filter"
require "fairy/node/n-single-exportable"

module Fairy
  class NHere<NFilter
    Processor.def_export self

    include NSingleExportable

    def basic_each(&block)
      @input.each do |e|
	block.call e
      end
    end
  end
end
