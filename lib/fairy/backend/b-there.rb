# encoding: UTF-8

require "controller"
require "backend/binput"

module Fairy
  class BThere<BInput
    Controller.def_export self

    def initialize(controller, opts, enumerable)
      super
      @enumerable = enumerable
    end

    def node_class_name
      "NThere"
    end

    def njob_creation_params
      [@enumerable]
    end

    def create_and_start_nodes
      begin
	no = 0
	@create_node_mutex.synchronize do
	  nthere = nil
	  @controller.assign_new_processor(self) do |processor|
	    nthere = create_node(processor)
	  end
	  no += 1
	  nthere.start
	end
      rescue BreakCreateNode
	# do nothing
	Log::debug self, "BREAK CREATE NODE: #{self}" 
      ensure
	self.number_of_nodes = no
      end
    end
  end
end


      
