# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-input"

module Fairy

  class CInputVArray<CInput
    Controller.def_export self

    def initialize(controller, opts, varray)
      super
      @varray = varray
    end

    def node_class_name
      "PInputVArray"
    end

    def start
      @cvarray_place = CVarrayPlace.new(@varray)
      start_create_nodes
    end

    def input
      @cvarray_place
    end

#     def create_and_start_nodes
#       begin
# 	no = 0
# 	@varray.arrays_size.times do 
# 	  @create_node_mutex.synchronize do
# 	    subarray = @varray.arrays_at(no)
# 	    @controller.assign_processor(self, 
# 					 :SAME_PROCESSOR_OBJ, 
# 					 subarray) do |processor|
# 	      njob = create_node(processor, subarray)
# 	      njob.start
# 	      no +=1
# 	    end
# 	  end
# 	end
#       rescue BreakCreateNode
# 	# do nothing
# 	Log::debug self, "BREAK CREATE NODE: #{self}" 
#       ensure
# 	self.number_of_nodes = no
#       end
  end
end
