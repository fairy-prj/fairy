require "uri"

require "backend/binput"

module Fairy
  class BFile<BInput
    def BFile.open(controller, descripter)
      descripter = descripter.to_a
      bfile = BFile.new(controller)
      bfile.open(descripter)
      bfile
    end

    URI_REGEXP = /:\/\//

    def open(descripter)
      no = 0
      for file in descripter
	no +=1

	host = "localhost"
	path = file
	if URI_REGEXP =~ file
	  uri = URI(file)
	  host = uri.host
	  path = uri.path
	end
	processor = @controller.assign_input_processor(self, host)
	node = processor.nfile_open(self, path)
# 	node_klass = processor.NFile
# 	node = node_klass.open(self, uri.path)
	add_node node
      end
      self.number_of_nodes = no
    end
  end
end
