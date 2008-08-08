require "forwardable"

require "deep-connect/deep-connect"

require "backend/bjob"
require "backend/b-inputtable"

module Fairy
  class BBarrier<BJob
    extend Forwardable

    include BInputtable

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL)")

    def initialize(controller, opts = nil)
      super(controller)
      for k, val in opts
	case k
	when :mode
	  @mode = BBarrierMode.create(self, val)
	when :cond
	  @cond = BBarrierCond.create(self, val)
	when :buffer
	  @buffer = BBarrierBuffer.create(self, val)
	else
	end
      end
    end

    def each_export(&block)
puts "EACH_EXPORT: WAIT"
      @mode.wait_exportable
puts "EACH_EXPORT: AWAKE"
      @buffer.each_export(&block)
    end

    def_delegator :@mode, :wait_export

    def_delegator :@cond, :wait_cond

#    def_delegator :@buffer, :start_create_node
    def_delegator :@buffer, :input=

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
	raise "そのモードはありません#{mode}" unless klass
	
	mode = klass.new(bbarrier, mode, *opts)
      end

      def register_mode(mode, klass)
	@ModeName2Class[mode] = klass
      end

    end

    module Mode
      def initialize(bbarrier, mode, *opts)
	@bbarrier = bbarrier
	@mode = mode
	super(*opts)
      end
    end
    
    #
    class BBarrierMode
      extend Factory
      include Mode
    end

    class BBarrierNodeCreationMode<BBarrierMode
      BBarrierMode.register_mode(:NODE_CREATION, self)

      def initialize(bbarrier, mode)
	super

	@mutex = Mutex.new
	@cv = ConditionVariable.new
      end

      def wait_exportable
	@bbarrier.wait_cond
      end

      def wait_export
	true
      end

    end

    class BBarrierStreamMode<BBarrierMode
      BBarrierMode.register_mode(:STREAM, self)

      def wait_exportable
	true
      end

      def wait_export
	@bbarrier.wait_cond
      end

    end

    #
    class BBarrierCond
      extend Factory
      include Mode
      
      def self.create(bbarrier, mode)
	if mode.kind_of?(String)
	  super(bbarrier, :BLOCK_COND, mode)
	else
	  super(bbarrier, mode)
	end
      end

      def wait_cond
	raise "まだできていません"
      end

    end

    class BBarrierNodeArrivedCond<BBarrierCond
      BBarrierCond.register_mode(:NODE_ARRIVED, self)

      def wait_cond
	@bbarrier.node_arrived?
      end

    end

    class BBarrierDataArrivedCond<BBarrierCond
      BBarrierCond.register_mode(:DATA_ARRIVED, self)

      def wait_cond
	@bbarrier.data_arrived?
      end

    end

    class BBarrierAllDataCond<BBarrierCond
      BBarrierCond.register_mode(:ALL_DATA, self)

      def wait_cond
	@bbarrier.all_data_imported?
      end
    end

    class BBarrierBlockCond<BBarrierCond
      BBarrierCond.register_mode(:BLOCK_COND, self)

      def initialize(bbarrier, mode, block_source)
	super(bbarrier, mode)
	@block_source = block_source
	@block = @bbarrier.instance_eval{@context.create_proc(block_source)}
      end

      def wait_cond
	@block.call
      end
    end

    #
    class BBarrierBuffer<BFilter
      extend Factory
      include Mode
      include BInputtable

    end

    class BBarrierMemoryBuffer<BBarrierBuffer
      BBarrierBuffer.register_mode(:MEMORY, self)

      def initialize(bbarrier, mode)
	super(bbarrier, mode, bbarrier.instance_eval{@controller})
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
	"NBarrierMemoryBuffer"
      end

      def wait_export
	@bbarrier.wait_export
      end

      def start_create_nodes
	super

#	start_watch_all_node_data_arrived
#	start_watch_all_node_data_imported
      end

      def create_node(processor)
	processor.create_njob(node_class_name, self)
      end

#       def start_watch_all_node_data_arrived
# 	Thread.start do
# 	  while !all_node_data_arrived?
# 	    @nodes_status_mutex.synchronize do
# 	      @nodes_status_cv.wait(@nodes_status_mutex)
# 	    end
# 	  end
# 	end
#       end

      def all_node_data_arrived?
	return false unless @number_of_nodes

	data_arrived = true
	each_node(:exist_only) do |node|
	  st = @nodes_status[node]
	  data_arrived &&= [:ST_ACTIVATE, :ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
	end
	data_arrived
      end

#       def start_watch_all_node_data_imported
# 	Thread.start do
# 	  while !all_node_data_imported?
# 	    @nodes_status_mutex.synchronize do
# 	      @nodes_status_cv.wait(@nodes_status_mutex)
# 	    end
# 	  end
# 	end
#       end

      def all_node_data_imported?
puts "ALL_NODE_DATA_IMPORTED?: 0"
	return false unless @number_of_nodes

	all_data_imported = true
	each_node(:exist_only) do |node|
	  st = @nodes_status[node]
puts "ALL_NODE_DATA_IMPORTED?: #{node} #{st}"
	  s = [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
puts "ALL_NODE_DATA_IMPORTED?: #{s}"
	  
	  all_data_imported &&= [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
	end
puts "ALL_NODE_DATA_IMPORTED?: #{all_data_imported}"
	all_data_imported
      end

    end

    class BBarrierFileBuffer<BBarrierBuffer
      BBarrierBuffer.register_mode(:FILE, self)

      def node_class_name
	"NBarrier::NBarrierFileBuffer"
      end

      def create_node(processor)
	processor.create_njob(node_class_name, self)
      end
    end

  end
end



