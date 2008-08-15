require "uri"

require "controller"
require "backend/binput"
require "share/vfile"

module Fairy
  class BFile<BInput
    Controller.def_export self

    def BFile.open(controller, opts, descripter)
      @descripter = descripter
      bfile = BFile.new(controller, opts)
      bfile.open(descripter)
      bfile
    end
    DeepConnect.def_single_method_spec(self, "REF open(REF, VAL, VAL)")

    URI_REGEXP = /:\/\//

    def node_class_name
      "NFile"
    end

    def open(vf)
      no = 0
      for file in vf
	no +=1

	host = "localhost"
	path = file
	if URI_REGEXP =~ file
	  uri = URI(file)
	  host = uri.host
	  if /^\[([0-9a-f.:]*)\]$/ =~ host
	    host = $1
	  end
	  path = uri.path
	end
	processor = @controller.assign_input_processor(self, host)
	node = create_node(processor)
	node.open(path)
      end
      self.number_of_nodes = no
    end
    DeepConnect.def_method_spec(self, "REF open(DVAL)")

  end
end
