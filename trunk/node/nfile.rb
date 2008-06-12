
require "node/njob"
require "node/port"

module Fairy
  class NFile<NJob

    def NFile.open(bjob, fn)
      nfile = NFile.new(bjob)
      nfile.open(fn)
    end

    def initialize(bjob)
      super
      @file = nil
      @export = Export.new
    end

    def open(file)
      @file = File.open(file)
      start
      self
    end

    def output=(output)
      @export.output = output.import
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
      end
    end
  end
end
