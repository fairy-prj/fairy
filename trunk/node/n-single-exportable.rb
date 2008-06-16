
module Fairy
  module NSingleExportable
    END_OF_STREAM = NJob::END_OF_STREAM

    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(*opts)
      super
      @export = Export.new
    end

    attr_reader :export

    def start(&block)
      super do
	block.call
	@export.push END_OF_STREAM
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
