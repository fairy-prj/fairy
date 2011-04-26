# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "xthread"

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"

module Fairy
  class CSegZip<CIOFilter
    Controller.def_export self

    ZIP_BY_SEGMENT = :ZIP_BY_SEGMENT

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, REF)")

    def initialize(controller, opts, others, block_source)
      super
      @others = others
      @block_source = block_source

      #@exports = [{o=>filter, ...}, ...]
      @exports = []
      @others_status = {}
      @exports_mutex = Mutex.new
      @exports_cv = XThread::ConditionVariable.new
    end

    def opt_zip_by_substream?
      @opts[ZIP_BY_SEGMENT]
    end

    def node_class_name
      "PSegZip"
    end

    def njob_creation_params
      [@block_source]
    end

    def start_create_nodes
      Log::debug self, "START_CREATE_NODES: #{self}"
      @others.each do |other|
	Thread.start do
	  other.each_assigned_filter do |input_filter|
	    @exports_mutex.synchronize do
	      unless exps = @exports[input_filter.no]
		exps = @exports[input_filter.no] = {}
		@exports_cv.broadcast
	      end
	      exp = input_filter.start_export
	      exps[other] = exp
	    end
	  end
	  @exports_mutex.synchronize do
	    @others_status[other] = true
	    @exports_cv.broadcast
	  end
	end
      end
      super
    end

    class NoAllFilter<Exception;end

    def create_and_add_node(ntask, mapper, opts={})
      unless opt_zip_by_substream?
 	ERR::Raise ERR::NoImplement, "except zip_by_segment"
      end

      node = create_node(ntask) {|node|
	if opts[:init_njob]
	  opts[:init_njob].call(node)
	end
	mapper.bind_input(node)

	no = node.no
	exps = nil
	@exports_mutex.synchronize do
	  while !(exps = other_filter_of(no))
	    @exports_cv.wait(@exports_mutex)
	  end
	end
 	node.zip_inputs = exps
 	exps.zip(node.zip_imports) do |other, import| 
	  other.output = import
	  import.no_import = 1
	end
      }
      node
    end

    def other_filter_of(no)
      begin
	@others.collect do |o| 
	  unless exp = @exports[no][o]
	    unless @other_status[o]
	      raise NoAllFilter
	    end
	  end
	  exp
	end
      rescue NoAllFilter
	return nil
      rescue
	return nil
      end
    end

    def break_running
      super
      @others.each{|others| Thread.start{others.break_running}}
    end


    class CPreSegZipFilter<CIOFilter
      Controller.def_export self

      def initialize(controller, opts)
	super
      end

      def node_class_name
	"PIdentity"
      end

      def njob_creation_params
	[]
      end
    end
  end
end
