# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "forwardable"

require "deep-connect/deep-connect"

require "fairy/master/c-filter"
require "fairy/master/c-inputtable"
require "fairy/master/c-io-filter"

require "fairy/share/block-source"

module Fairy
  class CBarrier<CIOFilter
    extend Forwardable

    Controller.def_export self

#    include BInputtable

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL)")

    def initialize(controller, opts)
      super
      for k, val in opts.dup
	case k
	when :mode
	  @mode = CBarrierMode.create(self, val, opts)
	when :cond
	  @cond = CBarrierCond.create(self, val, opts)
	when :buffer
	  @buffer = CBarrierBuffer.create(self, val, opts)
	else
	end
      end
    end

    def each_assigned_filter(&block)
      @mode.wait_exportable
      @buffer.each_assigned_filter(&block)
    end

    def_delegator :@mode, :wait_export

    def_delegator :@cond, :wait_cond

    def_delegator :@buffer, :input=
    def_delegator :@buffer, :output=

    def_delegator :@buffer, :node_arrived?
    def_delegator :@buffer, :data_arrived?
    def_delegator :@buffer, :all_data_imported?

    #
    module Factory
      def self.extended(mod)
	mod.init_fact
      end

      def init_fact
	@ModeName2Class = {}
      end
      
      def create(bbarrier, mode, *opts)
	klass = @ModeName2Class[mode]
	ERR::Raise ERR::NoSuchMode, mode unless klass
	
	mode = klass.new(bbarrier, mode, *opts)
	mode
      end

      def register_mode(mode, klass)
	@ModeName2Class[mode] = klass
      end
    end

    module Mode
      def initialize(bbarrier, mode, *opts)
	@bbarrier = bbarrier
	@mode = mode
	@opts = opts.last
#	Log::debug self, self.class.superclass.name
	begin
	  super(*opts)
	rescue
	  # for ruby 1.9.1
	  super()
	end
      end
    end
    
    #
    class CBarrierMode
      extend Factory
      include Mode

      def initialize(bbarrier, mode, *opts)
	super
	@opts = opts
      end

    end

    class CBarrierNodeCreationMode<CBarrierMode
      CBarrierMode.register_mode(:NODE_CREATION, self)

      def wait_exportable
	@bbarrier.wait_cond
      end

      def wait_export
	true
      end

    end

    class CBarrierStreamMode<CBarrierMode
      CBarrierMode.register_mode(:STREAM, self)

      def wait_exportable
	true
      end

      def wait_export
	@bbarrier.wait_cond
      end

    end

    #
    class CBarrierCond
      extend Factory
      include Mode
      
      def self.create(bbarrier, mode, opts=nil)
	if mode.kind_of?(BlockSource)
	  opts[:BLOCK_SOURCE] = mode
	  super(bbarrier, :BLOCK_COND, opts)
	else
	  super(bbarrier, mode, opts)
	end
      end

      def wait_cond
	ERR::Raise ERR::NoImpliment, "wait_cond"
      end

    end

    class CBarrierNodeArrivedCond<CBarrierCond
      CBarrierCond.register_mode(:NODE_ARRIVED, self)

      def wait_cond
	@bbarrier.node_arrived?
      end

    end

    class CBarrierDataArrivedCond<CBarrierCond
      CBarrierCond.register_mode(:DATA_ARRIVED, self)

      def wait_cond
	@bbarrier.data_arrived?
      end

    end

    class CBarrierAllDataCond<CBarrierCond
      CBarrierCond.register_mode(:ALL_DATA, self)

      def wait_cond
	@bbarrier.all_data_imported?
      end
    end

    class CBarrierBlockCond<CBarrierCond
      CBarrierCond.register_mode(:BLOCK_COND, self)

      def initialize(bbarrier, mode, opts)
	super(bbarrier, mode, opts)

	if @opts[:BEGIN]
	  bs = BScript.new(@opts[:BEGIN], 
			   @bbarrier.instance_eval{@context}, 
			   @bbarrier)
	  bs.evaluate
	end
	@block_source = @opts[:BLOCK_SOURCE]
	@block = BBlock.new(@block_source, 
			    @bbarrier.instance_eval{@context}, 
			    @bbarrier)
	# @opts[:END] は未サポート
      end

      def wait_cond
	@block.call
      end
    end

    #
    class CBarrierBuffer<BFilter
      extend Factory
      include Mode
    end

    class CBarrierMemoryBuffer<CBarrierBuffer
      CBarrierBuffer.register_mode(:MEMORY, self)

      def initialize(bbarrier, mode, opts=nil)
	super(bbarrier, mode, bbarrier.instance_eval{@controller}, opts)
      end

      def node_arrived?
	number_of_nodes
      end

      def data_arrived?
	@nodes_status_mutex.synchronize do
	  while !all_node_data_arrived?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	true
      end

      def all_data_imported?
	@nodes_status_mutex.synchronize do
	  while !all_node_data_imported?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	true
      end

      def node_class_name
	"PBarrierMemoryBuffer"
      end

      def wait_export
	@bbarrier.wait_export
      end

      def all_node_data_arrived?
	return false unless @number_of_nodes

	data_arrived = true
	each_node(:exist_only) do |node|
	  st = @nodes_status[node]
	  data_arrived &&= [:ST_ACTIVATE, :ST_ALL_IMPORTED, :ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
	end
	data_arrived
      end

      def all_node_data_imported?
	return false unless @number_of_nodes

	all_data_imported = true
	each_node(:exist_only) do |node|
	  st = @nodes_status[node]
	  s = [:ST_ALL_IMPORTED, :ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
	  all_data_imported &&= [:ST_ALL_IMPORTED, :ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
	end
	all_data_imported
      end

    end

    class CBarrierFileBuffer<CBarrierBuffer
      CBarrierBuffer.register_mode(:FILE, self)

      def node_class_name
	"PBarrier::PBarrierFileBuffer"
      end
    end

  end
end



