
require "fairy/backend/binput"

module Fairy

  class BInputVArray<BInput
    Controller.def_export self

    def initialize(controller, opts, varray)
      super
      @varray = varray
    end

    def node_class_name
      "NInputVArray"
    end

    def create_and_start_nodes
      begin
	no = 0
	@varray.arrays_size.times do 
	  @create_node_mutex.synchronize do
	    subarray = @varray.arrays_at(no)
	    @controller.assign_processor(self, 
					 :SAME_PROCESSOR_OBJ, 
					 subarray) do |processor|
	      njob = create_node(processor, subarray)
	      njob.start
	      no +=1
	    end
	  end
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
