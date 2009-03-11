# encoding: UTF-8

require "thread"
require "resolv"
require "ipaddr"

require "deep-connect/deep-connect"
#DeepConnect::Organizer.immutable_classes.push Array

require "share/conf"
require "logger"
require "share/log"

module Fairy

  class Master
    def initialize

#       @clients = {}
#       @clients_mutex = Mutex.new
#       @clients_cv = ConditionVariable.new

      @controller_seq = -1
      @controller_seq_mutex = Mutex.new

      @controllers = {}
      @controllers_mutex = Mutex.new
      @controllers_cv = ConditionVariable.new

#      @clientds2controller = {}

      @nodes = {}
      @nodes_mutex = Mutex.new

      @no_of_processors = {}
      @no_of_processors_mutex = Mutex.new
    end

    attr_reader :logger
    
    def start(service)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Master", self)

      @logger = Logger.new
      Log.logger = @logger
      Log.type = "[M]"

      @deepconnect.when_disconnected do |deepspace, opts|
	when_disconnected(deepspace, opts)
      end

      Log.info(self, "Master Service Start")
    end

    def when_disconnected(deepspace, opts)
      Log::info self, "MASTER: disconnected: Start termination"
#       @controllers_mutex.synchronize do
# 	if c = @controllers.find{|c| c.deep_space == deepspace}
# 	  when_disconnected_controller(c, deepspace, opts)
# 	end
#       end

      # node
    end

    # Controller 関連メソッド

    def controller_next_id
      @controller_seq_mutex.synchronize do
	@controller_seq += 1
      end
    end

    def assgin_controller

#       @clients_mutex.synchronize do
# 	@clients[fairy.deep_space] = fairy
#       end

      @controllers_mutex.synchronize do
	controller_id = controller_next_id
	Process.fork do
	  exec(CONF.RUBY_BIN, CONF.CONTROLLER_BIN,
	       "--master", @deepconnect.local_id.to_s, 
	       "--id", controller_id.to_s)
	end
	while !@controllers[controller_id]
	  @controllers_cv.wait(@controllers_mutex)
	end
#	@clientds2controller[fairy.deep_space] = @controllers[controller_id]
	@controllers[controller_id]
      end
    end

    def register_controller(controller)
      @controllers_mutex.synchronize do
	@controllers[controller.id] = controller
	@controllers_cv.broadcast
      end
    end

    def terminate_controller(controller)
      @controllers_mutex.synchronize do
	@controllers.delete(controller)
	@controllers_cv.broadcast
      end
      
      begin
	controller.terminate
	Process.wait
      rescue
	p $!, $@
      end
    end

    #
    def set_no_of_processors(node, no)
      @no_of_processors_mutex.synchronize do
	@no_of_processors[node] = no
      end
    end

    def leisured_node
      min_node = nil
      min_no_processor = nil
      for uuid, node in @nodes.dup
#	no = nil
#	@no_of_processors_mutex.synchronize do
	no = @no_of_processors[node]
#	end
	if !min_no_processor or min_no_processor > no
	  min_no_processor = no
	  min_node = node
	end
      end
      min_node
    end

    # Node 関連メソッド
    def register_node(node)
      @nodes_mutex.synchronize do
	@no_of_processors[node] = 0
	
	addr = node.deep_space.peer_uuid[0]
	@nodes[addr] = node
	Log::info self, "Node added: #{addr}->#{node}"
	node.addr = addr
      end
    end

    # IPv4(ipv6map) または IPv6アドレスか?
    IPADDR_REGEXP = /::ffff:([0-9]+\.){3}[0-9]+|[0-9a-f]+:([0-9a-f]*:)[0-9a-f]*/

    def node(host)
#puts "HOST: #{host}"
      unless IPADDR_REGEXP =~ host
	Resolv.each_addresses(host) do |addr|
	  ipaddr = IPAddr.new(addr)
	  ipaddr = ipaddr.ipv4_mapped if ipaddr.ipv4?
	  host = ipaddr.to_s

	  @nodes_mutex.synchronize do
	    return node if node = @nodes[host] 
	  end
	end
	
	return nil
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
