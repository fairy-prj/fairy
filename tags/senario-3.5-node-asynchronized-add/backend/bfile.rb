
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
      no = 0
      for fn in descripter
	no +=1
	node = NFile.open(self, fn)
	add_node node
      end
      self.number_of_nodes = no
    end
  end
end
