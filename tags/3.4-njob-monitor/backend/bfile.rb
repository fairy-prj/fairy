
require "backend/binput"
require "node/nfile"

module Fairy
  class BFile<BInput
    def BFile.open(controller, descripter)
      bfile = BFile.new(controller)
      bfile.open(descripter)
      bfile
    end

    def open(descripter)
      self.nodes = descripter.map{|fn| NFile.open(self, fn)}
    end
  end
end
