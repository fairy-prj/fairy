
require "node/n-filter"

module Fairy
  class NFilter1to1<NFilter
    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(bjob)
      super
      @export = Export.new
    end

    attr_reader :export

     def output=(output)
       @export.output= output
     end

    def start(&block)
      super do
	block.call
	wait_export_finish
      end
    end

    def wait_export_finish
      self.status = ST_WAIT_EXPORT_FINISH
      @export.wait_finish
      self.status = ST_EXPORT_FINISH
    end

  end
end
