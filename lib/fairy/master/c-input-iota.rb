# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-input"

module Fairy

  class CInputIota<CInput
    Controller.def_export self

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(controller, opts, last)
      super
      @last = last - 1
    end

    def node_class_name
      "PInputIota"
    end

    def start
      offset = 0
      offset = @opts[:offset] if @opts[:offset]
      split_no = @opts[:SPLIT_NO]

      @ciota_place = CIotaPlace.new(@last, offset, split_no)
      start_create_nodes
    end

    def input
      @ciota_place 
    end

#     def create_and_start_nodes
#       begin
# 	offset = 0
# 	offset = @opts[:offset] if @opts[:offset]
# 	split_no = @opts[:SPLIT_NO]

# 	first = offset
# 	no = 0
# 	split_no.times do
# 	  @create_node_mutex.synchronize do
# 	    no += 1
# 	    Log::debug self, "NO: #{no}"
# 	    last = [first + @last.div(split_no), @last].min
# 	    @controller.assign_processor(self, :NEW_PROCESSOR) do |processor|
# 	      njob = create_node(processor, first, last)
# 	      njob.start
# 	      first = last + 1
# 	    end
# 	  end
# 	  sleep 0.1
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
