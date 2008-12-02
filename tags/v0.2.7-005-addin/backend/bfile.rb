require "uri"

require "controller"
require "backend/binput"
require "share/vfile"

module Fairy
  class BFile<BInput
    Controller.def_export self

    def BFile.open(controller, opts, descripter)
      bfile = BFile.new(controller, opts)
      bfile.open(desctipter)
      bfile
    end
    DeepConnect.def_single_method_spec(self, "REF open(REF, VAL, VAL)")

    URI_REGEXP = /:\/\//

    def node_class_name
      "NFile"
    end

    def open(vf)
      @vfile = vf
      start
    end
    DeepConnect.def_method_spec(self, "REF open(DVAL)")

    def create_and_start_nodes
      begin
	no = 0
	for file in @vfile
	  @create_node_mutex.synchronize do
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
	    @controller.assign_input_processor(self, host) do |processor|
	      node = create_node(processor)
	      node.open(path)
	    end
	  end
	end
      rescue BreakCreateNode
	# do nothing
	Log::debug self, "BREAK CREATE NODE: #{self}" 
      rescue Exception
	Log::debug_exception(self)
	raise
      ensure
	Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	self.number_of_nodes = no
      end
    end
  end
end
