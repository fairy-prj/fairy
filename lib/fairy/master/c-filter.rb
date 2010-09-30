# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  class CFilter
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
      @nodes_for_next_filters = []
      @nodes_mutex = Mutex.new
      @nodes_cv = ConditionVariable.new

      @nodes_status = {}
      @nodes_status_mutex = Mutex.new
      @nodes_status_cv = ConditionVariable.new

      @controller.register_bjob(self)

      @create_node_thread = nil
      # gbreakのときに安全に@create_node_threadスレッドをとめるため
      @create_node_mutex = Mutex.new

      @context = Context.new(self)

      start_watch_node_status if watch_status?
    end

    def input
      ERR::Raise ERR::INTERNAL::ShouldDefineSubclass
    end

    def postmapping_policy
      @opts[:postmapping_policy] || CONF.POSTMAPPING_POLICY
    end

    #
    # Pool Variables:
    # (JP: プール変数)
    #
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
    # Njob Methods:
    #
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
	unless node
	  @nodes_for_next_filters.push nil
	  @nodes_cv.broadcast
	  return
	end

#	node.no = node.input.no
	@nodes[node.no] = node
	@nodes_for_next_filters.push node
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
	  begin
	    @nodes_mutex.unlock
	    block.call @nodes[idx] 
	  ensure
	    @nodes_mutex.lock
	  end
	  idx +=1
	end
      end
    end

    def each_node_exist_only(&block)
      nodes = @nodes_mutex.synchronize{@nodes.dup}
      nodes.each &block
    end

#     def each_export(&block)
#       each_node do |node|
# 	exp = node.export
# 	block.call exp, node
# 	node.export.output_no_import = 1
#       end
#     end

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

    #
    # Njob creation methods
    #
    def start_create_nodes
      @create_node_thread = Thread.start{
	Log::debug self, "START_CREATE_NODES: START #{self}"
	create_nodes
	Log::debug self, "START_CREATE_NODES: END #{self}"
      }
      nil
    end

    def create_nodes
      begin
	no = 0
	ret = nil
	@controller.assign_ntasks(self, @create_node_mutex) do 
	  |ntask, mapper, opts={}|
	  njob = create_and_add_node(ntask, mapper, opts)
	  no += 1
	  njob
	end
	add_node(nil)
	Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	self.number_of_nodes = no

      rescue BreakCreateNode
	Log::debug self, "BREAK CREATE NODE: #{self}" 
	add_node(nil)
	Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	self.number_of_nodes = no

      rescue AbortCreateNode
	Log::debug self, "Abort CREATE NODE: #{self}" 
	# do nothing

      rescue ERR::NodeNotArrived
	Log::debug self, "NODE NOT ARRIVED: #{self}"
	begin
	  handle_exception($!)
	rescue
	  Log::debug_exception(self)
	end
	Log::debug self, "NODE NOT ARRIVED2: #{self}"
	raise

      rescue Exception
	Log::warn_exception(self)
	raise
#      ensure
#	Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	#add_node(nil)
	#self.number_of_nodes = no
      end
    end

    def create_and_add_node(ntask, mapper, opts={})
      node = create_node(ntask) {|node|
	if opts[:init_njob]
	  opts[:init_njob].call(node)
	end
	mapper.bind_input(node)
      }
      node
    end

    def create_node(ntask, *params, &block)
      if params.empty?
	params = njob_creation_params
      end
      njob = ntask.create_njob(node_class_name, self, @opts, *params)
      block.call(njob)
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


#     def next_filter(mapper)
#       @nodes_mutex.synchronize do
# 	ret = nil
# 	while !ret
# 	  while @nodes_for_next_filters.empty?
# 	    @nodes_cv.wait(@nodes_mutex)
# 	  end
# 	  ret = @nodes_for_next_filters.shift
# 	  Log::debug(self, "NEXT_FILTER: #{ret}")
# 	end
# 	ret = nil if ret == :NIL
# 	ret
#       end
#     end

#     def next_filter(mapper)
#       @nodes_mutex.synchronize do
# 	while @nodes_for_next_filters.empty?
# 	  @nodes_cv.wait(@nodes_mutex)
# 	end
# 	@nodes_for_next_filters.shift
#       end
#     end

    def each_assigned_filter(&block)
      loop do 
	input_filter = nil
	@nodes_mutex.synchronize do
	  while @nodes_for_next_filters.empty?
	    @nodes_cv.wait(@nodes_mutex)
	  end
	  input_filter = @nodes_for_next_filters.shift
	  return unless input_filter
	end
	block.call input_filter
      end 
    end

    def start_export(njob)
      export = njob.start_export
    end

    def create_import(processor)
      processor.create_import(@opts[:prequeuing_policy])
    end

    def each_export_by(njob, mapper, &block)
#      block.call njob.export, :foo=>:bar
      block.call njob.export
    end

    def bind_export(exp, imp)
      imp.no_import = 1
    end

    #
    # job control
    #
    def break_running(njob = nil)
      break_create_node
      
      each_node do |tasklet|
	tasklet.break_running unless tasklet.equal?(njob)
      end
    end

    def break_create_node
      # 作成中のものは完全に作成させるため
      @controller.create_processor_mutex.synchronize do
	if @create_node_thread && @create_node_thread.alive?
	  @create_node_thread.raise BreakCreateNode
	end
      end
    end

    def abort_create_node
Log::debug(self, "ABORT_CREATE_NODE: S")
      @controller.create_processor_mutex.synchronize do
Log::debug(self, "ABORT_CREATE_NODE: 1")
	if @create_node_thread && @create_node_thread.alive?
Log::debug(self, "ABORT_CREATE_NODE: 2 ")
	  @create_node_thread.raise AbortCreateNode
Log::debug(self, "ABORT_CREATE_NODE: 3")
	end
Log::debug(self, "ABORT_CREATE_NODE: E")
      end
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
