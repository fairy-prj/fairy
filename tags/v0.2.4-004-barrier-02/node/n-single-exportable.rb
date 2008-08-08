 
module Fairy
  module NSingleExportable
    END_OF_STREAM = NJob::END_OF_STREAM

    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(processor, bjob, *opts)
      super(processor, bjob)
      @export = Export.new unless @export
    end

    attr_reader :export

    def no=(no)
      super
      @export.no = no
    end

    def start(&block)
      super do
	if @import
	  @export.add_key(@import.key)
	end
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
