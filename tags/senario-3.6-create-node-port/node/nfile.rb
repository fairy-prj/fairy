
require "node/njob"
require "node/port"

module Fairy
  class NFile<NJob
    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def NFile.open(bjob, fn)
      nfile = NFile.new(bjob)
      nfile.open(fn)
    end

    def initialize(bjob)
      super
      @file = nil
      @export = Export.new
    end

    attr_reader :export

    def open(file_name)
      @file_name = file_name
      @file = File.open(file_name)
      start
      self
    end

    def output=(output)
      @export.output = output
    end
    
#     def pop
#       @export_queue.pop
#     end
    
    def start
      super do
	for l in @file
	  @export.push l
	end
	@export.push END_OF_STREAM
	@file.close
	@file = nil # FileオブジェクトをGCの対象にするため

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
