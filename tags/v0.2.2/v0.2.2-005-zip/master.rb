
require "thread"
require "resolv"
require "ipaddr"

require "deep-connect/deep-connect"

module Fairy

  class Master

    CONTROLLER_BIN = "bin/controller"

    def initialize
      @controllers = []
      @controllers_mutex = Mutex.new
      @controllers_cv = ConditionVariable.new

      @nodes = {}
      @nodes_mutex = Mutex.new

      @processors = {}
      @processors_mutex = Mutex.new
    end

    
    def start(service)
      @deepconnect = DeepConnect.start(service)
      
      @deepconnect.export("Master", self)
    end

    # Controller 関連メソッド
    def assgin_controller
      @controllers_mutex.synchronize do
	controller_id = @controllers.size
	Process.fork do
	  exec(CONTROLLER_BIN,
	       "--master", @deepconnect.local_id.to_s, 
	       "--id", controller_id.to_s)
	end
	while !@controllers[controller_id]
	  @controllers_cv.wait(@controllers_mutex)
	end
	@controllers[controller_id]
      end
    end

    def register_controller(controller)
      @controllers_mutex.synchronize do
	@controllers[controller.id] = controller
	@controllers_cv.broadcast
      end
    end

    # Processor 関連メソッド
    # Policy: :SAME_PROCESSOR, :NEW_PROCESSOR, :INPUT, MUST_BE_SAME_PROCESSOR
    def assign_processor(policy, *opts)
      case policy
      when :INPUT
	assign_input_processor(opts[0])
      when :SAME_PROCESSOR, :MUST_BE_SAME_PROCESSOR
	processor = opts[0]
	processor
      when :NEW_PROCESSOR
	assign_new_processor
      else
	raise "未サポートのポリシー: #{policy}"
      end
    end

    def assign_input_processor(host)
puts "NODES: #{@nodes}"
      node = node(host)
      unless node
	raise "#{host} のホスト上でnodeが立ち上がっていません"
      end

      processor = node.assign_processor
      @processors_mutex.synchronize do
	@processors[node] = [] unless @processors[node]
	@processors[node].push processor
      end
      processor
    end

    def assign_new_processor
      min_node = nil
      min_no_processor = nil
      @processors_mutex.synchronize do
	for n, procs in @processors
	  if !min_no_processor or min_no_processor > procs.size
	    min_no_processor = procs.size
	    min_node = n
	  end
	end
      end
      processor = min_node.assign_processor
      @processors_mutex.synchronize do
	@processors[min_node] = [] unless @processors[min_node]
	@processors[min_node].push processor
      end
      processor
    end

    # Node 関連メソッド
    def register_node(node)
      @nodes_mutex.synchronize do
	addr = node.deep_space.peer_uuid[0]
	@nodes[addr] = node
	puts "Node added: #{addr}->#{node}"
      end
    end

    IPADDR_REGEXP = /::ffff:([0-9]+\.){3}[0-9]+|[0-9a-f]+:([0-9a-f]*:)[0-9a-f]*/

    def node(host)
puts "HOST: #{host}"
      unless IPADDR_REGEXP =~ host
	addr = Resolv.getaddress(host)
	ipaddr = IPAddr.new(addr)
	ipaddr = ipaddr.ipv4_mapped if ipaddr.ipv4?
	host = ipaddr.to_s
      end
      
      node = nil
      @nodes_mutex.synchronize do
	node = @nodes[host]
      end
      node
    end

    def Master.start(service)
      master = Master.new
      master.start(service)
    end
  end
end
