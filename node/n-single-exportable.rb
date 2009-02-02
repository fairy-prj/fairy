# encoding: UTF-8

require "node/n-filter"
 
module Fairy
  module NSingleExportable
    END_OF_STREAM = NJob::END_OF_STREAM

    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(processor, bjob, opts=nil, *rests)
      super
      @export = Export.new unless @export
    end

    attr_reader :export

    def no=(no)
      super
      @export.no = no
    end

    def start(&block)
      super do
	begin
	  if @import
	    @export.add_key(@import.key)
	  end
	  block.call
	ensure
	  @export.push END_OF_STREAM
	  wait_export_finish
	end
      end
    end

    def wait_export_finish
      self.status = ST_WAIT_EXPORT_FINISH
      @export.wait_finish
      self.status = ST_EXPORT_FINISH
    end
  end

  class NSingleExportFilter<NFilter
    include NSingleExportable
  end

  class NSingleExportInput<NFilter
    include NSingleExportable
  end
end
