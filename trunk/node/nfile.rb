
require "node/njob"
require "node/port"

module Fairy
  class NFile<NJob

    def NFile.open(fn)
      nfile = NFile.new
      nfile.open(fn)
    end

    def initialize
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
      Thread.start do
	for l in @file
	  @export.push l
	end
	@export.push END_OF_STREAM
      end
    end
  end
end
