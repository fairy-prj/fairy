# encoding: UTF-8

require "delegate"

require "thread"

require "fairy/backend/bjob"
require "fairy/backend/b-filter"

module Fairy
  class BCat<BFilter
    Controller.def_export self

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(controller, opts, others)
      super
      
      @others = others
      @export_node_pairs_queues = nil
      @export_node_pairs_queues_mutex = Mutex.new
      @export_node_pairs_queues_cv = ConditionVariable.new

      @main_precat = BPreCat.new(controller, opts)

      @others_precat = @others.map{|o| 
	precat = BPreCat.new(controller, opts)
	precat.input = o
	precat
      }
    end

    def node_class_name
      "NIdentity"
    end

    def njob_creation_params
      []
    end
      
    def input=(input)
      @main_precat.input = input
#      super(@main_precat)
      start_create_nodes
    end

    def create_nodes
      begin
	no = 0
	[@main_precat, *@others_precat].each do |input|
	  @input = input
	  input.output = @input
	  @controller.assign_processors(self, @create_node_mutex) do
	    |processor, mapper, opts={}|
#	    njob = create_and_add_node(processor, mapper)
	    njob = create_node(processor) {|node|
	      if opts[:init_njob]
		opts[:init_njob].call(node)
	      end
	      mapper.bind_input(node)
	      node.no = no
	    }
	    no += 1
	    njob
	  end
	end
      rescue BreakCreateNode
	# do nothing
	Log::debug self, "BREAK CREATE NODE: #{self}" 
      rescue ERR::NodeNotArrived
	Log::debug self, "NODE NOT ARRIVED: #{file}"
	begin
	  handle_exception($!)
	rescue
	  Log::debug_exception(self)
	end
	Log::debug self, "NODE NOT ARRIVED2: #{file}"
	raise

      rescue Exception
	Log::warn_exception(self)
	raise
      ensure
	Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	add_node(nil)
	self.number_of_nodes = no
      end
    end

#     def start_get_exports
#       @export_node_pairs_queues = [@input, *@others].collect{|input|
# 	export_node_pairs = Queue.new
# 	Thread.start do
# 	  input.each_export do |*export_node_pair|
# 	    export_node_pairs.push export_node_pair
# 	  end
# 	  export_node_pairs.push nil
# 	end
# 	export_node_pairs
#       }
#       @export_node_pairs_queues_cv.broadcast
#     end

#     def each_export(&block)
#       @export_node_pairs_queues_mutex.synchronize do
# 	while !@export_node_pairs_queues
# 	  @export_node_pairs_queues_cv.wait(@export_node_pairs_queues_mutex)
# 	end
#       end

#       for export_node_pairs in @export_node_pairs_queues
# 	while pair = export_node_pairs.pop
# 	  block.call *pair
# 	end
#       end
#     end

    class BPreCat<BFilter
      def initialize(controller, opts)
	super
      end

      def node_class_name
	"NIdentity"
      end

      def njob_creation_params
	[]
      end
    end

  end
end
