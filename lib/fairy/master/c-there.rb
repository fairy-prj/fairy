# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-input"

module Fairy
  class CThere<CInput
    Controller.def_export self

    def initialize(controller, opts, enumerable)
      super
      @enumerable = enumerable
    end

    def node_class_name
      "PThere"
    end

    def njob_creation_params
#      [@enumerable]
      []
    end

    def start
      @cthere_place = CTherePlace.new(@enumerable)
      start_create_nodes
    end

    def input
      @cthere_place
    end

#     def create_and_start_nodes
#       begin
# 	no = 0
# 	@create_node_mutex.synchronize do
# 	  nthere = nil
# 	  @controller.assign_new_processor(self) do |processor|
# 	    nthere = create_node(processor)
# 	  end
# 	  no += 1
# 	  nthere.start
# 	end
#       rescue BreakCreateNode
# 	# do nothing
# 	Log::debug self, "BREAK CREATE NODE: #{self}" 
#       ensure
# 	self.number_of_nodes = no
#       end
#     end
  end
end


      
