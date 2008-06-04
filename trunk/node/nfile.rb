

module Fairy
  class NFile

    def NFile.open(fn)
      nfile = NFile.new
      nfile.open(fn)
    end

    def initialize
      @file = nil

      @export_queue = SizedQueue.new(10)
    end

    def open(file)
      @file = File.open(file)
      start
      self
    end

    def pop
      @export_queue.pop
    end
    
    def start
      Thread.start do
	for l in @file
	  @export_queue.push l
	end
	@export_queue.push nil
      end
    end
  end
end
