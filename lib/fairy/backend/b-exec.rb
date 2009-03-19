# encoding: UTF-8

require "uri"

require "fairy/controller"
require "fairy/backend/binput"
require "fairy/share/vfile"


module Fairy

  class BExec<BInput
    Controller.def_export self

    URI_REGEXP = /:\/\//

    def node_class_name
      "NExec"
    end

    def start(vf)
      @vfile = vf
      super()
    end

    def create_and_start_nodes
      begin
	no = 0
	for node_spec in @vfile
	  @create_node_mutex.synchronize do
	    no += 1
	    Log::debug self, "NO: #{no}"
	    host = "localhost"
	    path = node_spec
	    if URI_REGEXP =~ node_spec
	      uri = URI(node_spec)
	      host = uri.host
	      if /^\[([0-9a-f.:]*)\]$/ =~ host
		host = $1
	      end
	      path = uri.path
	    end

	    @controller.assign_input_processor(self, host) do |processor|
	      njob = create_node(processor)
	      njob.start(node_spec)
	    end
	  end
	end
      rescue BreakCreateNode
	# do nothing
	Log::debug self, "BREAK CREATE NODE: #{self}" 
      rescue Exception
	Log::warn_exception(self)
	raise
      ensure
	Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	self.number_of_nodes = no
      end
    end
  end
end
