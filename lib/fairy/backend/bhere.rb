# encoding: UTF-8

require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"

require "fairy/node/port"


module Fairy
  class BHere<BFilter
    Controller.def_export self

    def initialize(controller, opts=nil)
      
      @imports = Queue.new
      super
    end

    def create_nodes
      super

      @imports.push nil
    end

    def create_and_add_node(export, bjob)
      node = super(export, bjob)
      policy = @opts[:prequeuing_policy]
      import = Import.new(policy)
      import.set_log_callback do |n| 
	Log::verbose(self, "IMPORT POP: #{n}")
      end

      @imports.push import
      node.export.output = import
      import.no_import = 1
    end

    def node_class_name
      "NHere"
    end

    def each(&block)
      while import = @imports.pop
	import.each do |e|
	  #[REQ#89]
	  block.call e
	end
      end
    end
    DeepConnect.def_method_spec(self, "each(){DVAL}")

    def each_buf(&block)
      threshold = @opts[:pool_threshold]
      threshold = CONF.HERE_POOL_THRESHOLD unless threshold
      chunk = []
      while import = @imports.pop
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


