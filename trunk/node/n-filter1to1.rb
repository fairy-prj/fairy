
require "node/n-filter"

module Fairy
  class NFilter1to1<NFilter
    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH

    def initialize(bjob)
      super
      @export = Export.new
    end

    def output=(output)
      @export.output= output.import
    end

    def start(&block)
      super do
	block.call
	wait_export_finish
      end
    end

    def wait_export_finish
      @export.wait_finish
    end

  end
end
