# encoding: UTF-8

require "fairy/controller"

module Fairy
  class BJob
    Controller.def_export self

    @@watch_status = false
    def self.watch_status
      @@watch_status
    end

    def self.watch_status=(val)
      @@watch_status=val
    end

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, *DEFAULT)")

    def initialize(controller, opts, *rests)
      Log::info self, "CREATE BJOB: #{self.class}"
      @controller = controller

      @opts = opts
      @opts = {} unless @opts

      @job_pool_dict = PoolDictionary.new

      @number_of_nodes = nil
#      @number_of_nodes_mutex = Mutex.new
#      @number_of_nodes_cv = ConditionVariable.new

      @nodes = []
      @nodes_mutex = Mutex.new
      @nodes_cv = ConditionVariable.new

      @nodes_status = {}
      @nodes_status_mutex = Mutex.new
      @nodes_status_cv = ConditionVariable.new

      @context = Context.new(self)

      start_watch_node_status if watch_status?
    end

    # プール変数
    def pool_dict
      @controller.pool_dict
    end

    def job_pool_dict
      @job_pool_dict
    end

    def def_job_pool_variable(vname, value = nil)
      @job_pool_dict.def_variable(vname, value)
    end

    def job_pool_variable(vname, *value)
      if value.empty?
	@job_pool_dict[vname]
      else
	@job_pool_dict[vname] = value
      end
    end

    #
    def create_node(processor, *params)
      if params.empty?
	params = njob_creation_params
      end
      njob = processor.create_njob(node_class_name, self, @opts, *params)
      add_node(njob)
      njob
    end
    
    def node_class_name
      ERR::Raise ERR::INTERNAL::NoRegisterService, self.class
    end

    def njob_creation_params
      []
    end

    def assgin_number_of_nodes?
      @number_of_nodes
    end

    def number_of_nodes
#      @number_of_nodes_mutex.synchronize do
      @nodes_mutex.synchronize do
	while !@number_of_nodes
#	  @number_of_nodes_cv.wait(@number_of_nodes_mutex)
	  @nodes_cv.wait(@nodes_mutex)
	end
	@number_of_nodes
      end
    end

    def number_of_nodes=(no)
#puts "#{self}.number_of_nodes=#{no}"
#      @number_of_nodes_mutex.synchronize do
      @nodes_mutex.synchronize do
	@number_of_nodes = no
#	@number_of_nodes_cv.broadcast
	@nodes_cv.broadcast
	@nodes_status_cv.broadcast
      end
    end

    def nodes
      @nodes_mutex.synchronize do
	@nodes
      end
    end

    def add_node(node)
      @nodes_mutex.synchronize do
	node.no = @nodes.size
	@nodes.push node
	@nodes_cv.broadcast
      end
    end

    def each_node(flag = nil, &block)
      if flag == :exist_only
	return each_node_exist_only &block
      end
      @nodes_mutex.synchronize do
	idx = 0
	while !@number_of_nodes || idx < @number_of_nodes
	  unless @nodes[idx]
	    @nodes_cv.wait(@nodes_mutex)
	    next
	  end
	  block.call @nodes[idx] 
	  idx +=1
	end
      end
    end

    def each_node_exist_only(&block)
      nodes = @nodes_mutex.synchronize{@nodes.dup}
      nodes.each &block
    end

    def each_export(&block)
      each_node do |node|
	exp = node.export
	block.call exp, node
	node.export.output_no_import = 1
      end
    end

    def break_running(njob = nil)
      break_create_node
      
      each_node do |tasklet|
	tasklet.break_running unless tasklet.equal?(njob)
      end
    end

    def break_create_node
      # 作成中のものは完全に作成させるため
Log::debug self, "BREAK_CREATE_NODE: #1"
      @create_node_mutex.synchronize do
Log::debug self, "BREAK_CREATE_NODE: #2"
	if @create_node_thread && @create_node_thread.alive?
	  @create_node_thread.raise BreakCreateNode
	end
Log::debug self, "BREAK_CREATE_NODE: #3"
      end
Log::debug self, "BREAK_CREATE_NODE: #4"
    end

    def abort_create_node
Log::debug self, "ABORT CREATE NODE: start"
      @create_node_mutex.synchronize do
	if @create_node_thread && @create_node_thread.alive?
	  @create_node_thread.kill
	end
      end
Log::debug self, "ABORT CREATE NODE: end"
    end      

    def update_status(node, st)
      @nodes_status_mutex.synchronize do
	@nodes_status[node] = st
	@nodes_status_cv.broadcast
      end
    end

    def watch_status?
      @@watch_status
    end

    def start_watch_node_status
      Thread.start do

	all_finished = false
	while !@number_of_nodes || !all_finished
	  @nodes_status_mutex.synchronize do
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end

	  all_finished = @number_of_nodes
	  Log::info(self) do |sio|
	    sio.puts "Status Changed: BEGIN #{self}"
	    each_node(:exist_only) do |node|
	      st = @nodes_status[node]
	      sio.puts "  node: #{node} status: #{st.id2name}" if st
	      all_finished &&= st==:ST_FINISH
	    end
	    sio.puts "Status Changed: END #{self}"
	  end
	end
	Log::info self, "Monitoring finish: ALL NJOB finished"
      end
      nil
    end

    def handle_exception(exp)
      @controller.handle_exception(exp)
    end

    class Context
      def initialize(bjob)
	@Pool = bjob.instance_eval{pool_dict}
	@JobPool = bjob.instance_eval{job_pool_dict}
	@__context = context
      end

#      def create_proc(source)
#	eval("proc{#{source}}", binding)
#      end

      def context
	__binding
      end

      class GlobalBreak<Exception;end
      def global_break
	Thread.current.raise GlobalBreak
      end
      alias gbreak global_break

      alias __binding binding
      def binding
	@__context
      end
      alias bind binding
    end
  end
end
