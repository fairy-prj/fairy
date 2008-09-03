
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

    def start
      self.number_of_nodes = @varray.arrays_size
      @varray.arrays_size.times do |idx|
	subarray = @varray.arrays_at(idx)
	@controller.assign_processor(self, 
				     :SAME_PROCESSOR_OBJ, 
				     subarray) do |processor|
	  njob = create_node(processor, subarray)
	  njob.start
	end
      end
    end
  end
end
