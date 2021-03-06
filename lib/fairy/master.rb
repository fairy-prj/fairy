# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "thread"
require "resolv"
require "ipaddr"

require "xthread"
require "fiber-mon"


require "deep-connect"
#DeepConnect::Organizer.immutable_classes.push Array

require "fairy/version"
require "fairy/share/conf"
require "fairy/logger"

module Fairy

  class Master
    def initialize

#       @clients = {}
#       @clients_mutex = Mutex.new
#       @clients_cv = XThread::ConditionVariable.new

      @controller_seq = -1
      @controller_seq_mutex = Mutex.new

      @controllers = {}
      @controllers_mutex = Mutex.new
      @controllers_cv = XThread::ConditionVariable.new

#      @clientds2controller = {}

      @nodes = {}
      @nodes_mutex = Mutex.new
      @node_seq = -1

      @no_of_processors = {}
      @no_of_processors_mutex = Mutex.new

      @no_of_active_processors = {}
      @no_of_active_processors_mutex = Mutex.new
      @no_of_active_processors_cv = XThread::ConditionVariable.new

    end

    attr_reader :controllers
    attr_reader :nodes

    attr_reader :logger

    def log_id
      "Master"
    end
    
    def start(service)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Master", self)

      require "fairy/share/inspector"
      @deepconnect.export("Inspector", Inspector.new(self))
      

      require "fairy/share/log"
      @logger = Logger.new
      Log.logger = @logger
      Log.type = "[M]"

      @deepconnect.when_disconnected do |deepspace, opts|
	when_disconnected(deepspace, opts)
      end

      Log.info(self, "Master Service Start")
      Log::info(self, "\tfairy version: #{Version}")
      Log::info(self, "\t[Powerd By #{RUBY_DESCRIPTION}]") 

      begin
	require "fairy.so"
	Log::warn self, "\t Load fairy.so"
      rescue LoadError
	Log::warn self, "Can't load fairy.so. Can't use this feature"
      end
    end

    def when_disconnected(deepspace, opts)
      Log::debug self, "MASTER: disconnected: Start termination"
#       @controllers_mutex.synchronize do
# 	if c = @controllers.find{|c| c.deep_space == deepspace}
# 	  when_disconnected_controller(c, deepspace, opts)
# 	end
#       end

      # node

      @nodes_mutex.synchronize do
	if addr_node= @nodes.find{|addr, node| node.deep_space == deepspace}
	  Log::info self, "MASTER: disconnected NODE: start termination"
	  when_disconnected_node(addr_node[0], addr_node[1], opts)
	end
      end
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

      Log::debug(self, "Assgin Controller")
      @controllers_mutex.synchronize do
	controller_id = controller_next_id
	MasterAPP.start_subcommand(CONF.RUBY_BIN, 
				   CONF.CONTROLLER_BIN,
				   "--master", @deepconnect.local_id.to_s, 
				   "--id", controller_id.to_s)
	begin
	  timeout(CONF.SUBCMD_EXEC_TIMEOUT) do
	    while !@controllers[controller_id]
	      @controllers_cv.wait(@controllers_mutex)
	    end
	  end
	rescue Timeout::Error
	  Log::fatal(self, "Can't exec Controller")
	  ERR::Fail ERR::CantExecSubcmd, "controller"
	end

#	@clientds2controller[fairy.deep_space] = @controllers[controller_id]
	Log::debug(self, "Assgin Controller: Assgined")
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
	@controllers.delete(controller.id)
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

    #
    def set_no_of_active_processors(node, no)
      @no_of_active_processors_mutex.synchronize do
	Log::debug(self, "CHANGE ACTIVE PROCESSORS: #{node}->#{no}")
	@no_of_active_processors[node] = no
	@no_of_active_processors_cv.broadcast
      end
    end

    def node_in_reisured(host)
Log::debug(self, "NODE IN LAISURED S:")
      node = node(host)
      
      return nil unless node

      @no_of_active_processors_mutex.synchronize do
	while @no_of_active_processors[node] > CONF.MASTER_MAX_ACTIVE_PROCESSORS
Log::debug(self, "NODE IN LAISURED 1: WAITING")
	  @no_of_active_processors_cv.wait(@no_of_active_processors_mutex)
Log::debug(self, "NODE IN LAISURED 2: WAITING END")
	end
Log::debug(self, "NODE IN LAISURED E:")
	node
      end
    end

    def node_in_reisured_without_block(host)
      node = node(host)
      @no_of_active_processors_mutex.synchronize do
	if @no_of_active_processors[node] > CONF.MASTER_MAX_ACTIVE_PROCESSORS
          return false
	end
      end
      node
    end

    def leisured_node(blocking = true)
Log::debug(self, "LAISURED NODE S:")
      @no_of_active_processors_mutex.synchronize do
	loop do
	  min_node = nil
	  min_no_processor = nil
	  for uuid, node in @nodes.dup
	    no = @no_of_active_processors[node]
	    if !min_no_processor or min_no_processor > no
	      min_no_processor = no
	      min_node = node
	    end
	  end
	  if min_no_processor <= CONF.MASTER_MAX_ACTIVE_PROCESSORS
Log::debug(self, "LAISURED NODE E:")
	    return min_node 
	  end
          if blocking
            Log::debug(self, "LAISURED NODE 1 WAITING:")
            @no_of_active_processors_cv.wait(@no_of_active_processors_mutex)
            Log::debug(self, "LAISURED NODE 2 WAITING END:")
          else
Log::debug(self, "LAISURED NODE E:")
            return nil
          end
	end
      end
    end

    def leisured_node_except_nodes(except_nodes = [], blocking = true)
Log::debug(self, "LAISURED NODE S:")
      @no_of_active_processors_mutex.synchronize do
	loop do
	  min_node = nil
	  min_no_processor = nil
	  for uuid, node in @nodes.dup
	    next if except_nodes.include?(node)

	    no = @no_of_active_processors[node]
	    if !min_no_processor or min_no_processor > no
	      min_no_processor = no
	      min_node = node
	    end
	  end
	  if min_node && min_no_processor <= CONF.MASTER_MAX_ACTIVE_PROCESSORS
Log::debug(self, "LAISURED NODE E:")
	    return min_node 
	  end
	  if blocking
            Log::debug(self, "LAISURED NODE 1 WAITING:")
            @no_of_active_processors_cv.wait(@no_of_active_processors_mutex)
            Log::debug(self, "LAISURED NODE 2 WAITING END:")
	  else
	    return nil
	  end
	end
      end
    end

    def unlimited_leisured_node
      min_node = nil
      min_no_processor = nil
      for uuid, node in @nodes.dup
#	no = nil
#	@no_of_processors_mutex.synchronize do
	no = @no_of_active_processors[node]
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
	@node_seq += 1
	@no_of_processors[node] = 0
	@no_of_active_processors[node] = 0
	
	addr = node.deep_space.peer_uuid[0]
	@nodes[addr] = node
	Log::info self, "Node added: #{addr}->#{node}##{@node_seq}"
	node.id = @node_seq
	node.addr = addr
      end
    end

    # IPv4(ipv6map) または IPv6アドレスか?
    IPADDR_REGEXP = /(::ffff:)?([0-9]+\.){3}[0-9]+|[0-9a-f]+:([0-9a-f]*:)[0-9a-f]*/

    def node(host)
#puts "HOST: #{host}"
      unless IPADDR_REGEXP =~ host
	Resolv.each_address(host) do |addr|
	  ipaddr = IPAddr.new(addr)
#	  ipaddr = ipaddr.ipv4_mapped if ipaddr.ipv4?
	  ipaddr = ipaddr.native
	  host = ipaddr.to_s

	  @nodes_mutex.synchronize do
	    if n = @nodes[host] 
	      return n 
	    end
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

    def when_disconnected_node(addr, node, opts)
#      addr = deep_space.peer_uuid[0]
      Log::info(self, "NODE: disconnected(#{addr})")
      @nodes.delete(addr)
    end

    def Master.start(service)
      master = Master.new
      master.start(service)
    end
  end
end



