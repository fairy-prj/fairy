# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"

require "fairy/share/port"


module Fairy
  class CHere<CIOFilter
    Controller.def_export self

    def node_class_name
      "PHere"
    end

    def create_and_add_node(ntask, mapper, opts={})
      node = create_node(ntask) {|node|
	if opts[:init_njob]
	  opts[:init_njob].call(node)
	end
	mapper.bind_input(node)
#	exp = node.start_export
      }
      node
    end

    def each(&block)
      policy = @opts[:prequeuing_policy]
      each_node do |node|
	node.start_export
	import = Import.new(policy)
	import.set_log_callback do |n, key| 
	  Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
	end
	import.no_import = 1
	node.export.output = import
	import.each do |e|
	  block.call e
	end
      end
    end
    DeepConnect.def_method_spec(self, "each(){DVAL}")

    def each_buf(&block)
      threshold = @opts[:pool_threshold]  || CONF.HERE_POOL_THRESHOLD
      chunk = []

      policy = @opts[:prequeuing_policy]
      each_node do |node|
	node.start_export
	import = Import.new(policy)
	import.set_log_callback do |n, key| 
	  Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
	end
	import.no_import = 1
	node.export.output = import

	import.each do |e|
	  #[REQ#89]

	  chunk.push e
	  if chunk.size > threshold
	    block.call chunk
	    chunk.clear
	  end
	end
      end
      if !chunk.empty?
	block.call chunk
      end
    end
    DeepConnect.def_method_spec(self, "each_buf(){DVAL}")
  end
end


