# encoding: UTF-8

require "fairy/share/exceptions"
require "fairy/backend/b-inputtable"

module Fairy
  module BInputtable

    # create_nodes init_njob: {|node| initialize of node}
    def create_nodes(opts = {})
      begin
	no = 0
	#	  @input.each_export do |export, node, opts|
	@input.each_export do |export, node, opts={}|
	  @create_node_mutex.synchronize do
	    new_n = create_and_add_node(export, node)
	    no += 1
	    if opts[:init_njob]
	      opts[:init_njob].call(new_n)
	    end
	  end
	end

      rescue BreakCreateNode
	# do nothing
	Log::debug self, "CAUGHT EXCEPTION: BreakCreateNode: #{self}" 
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
