# encoding: UTF-8

require "thread"
require "forwardable"

require "deep-connect/deep-connect.rb"

require "fairy/version"
require "fairy/share/conf"
require "fairy/share/pool-dictionary"
require "fairy/share/stdout"

module Fairy

  class Controller

    EXPORTS = []
    def Controller.def_export(obj, name = nil)
      unless name
	if obj.kind_of?(Class)
	  if /Fairy::(.*)$/ =~ obj.name
	    name = $1
	  else
	    name = obj.name
	  end
	else
	  ERR::Raise ERR::INTERNAL::CantDefExport, obj.to_s
	end
      end

      EXPORTS.push [name, obj]
    end
    
    def Controller.start(id, master_port)
      controller = Controller.new(id)
      controller.start(master_port)
    end

    def initialize(id)
      @id = id

      @deepconnect = nil

      @master_deepspace = nil
      @master = nil

      @client = nil

      @stdout_mutex = Mutex.new

      @services = {}

      @create_processor_mutex = Mutex.new

      # processor -> no of reserve 
      @reserves = {}
      @reserves_mutex = Mutex.new
      @reserves_cv = ConditionVariable.new

      # bjob -> [processor, ...]
      @bjob2processors = {}
      @bjob2processors_mutex = Mutex.new
      @bjob2processors_cv = ConditionVariable.new

      # processor -> no of active ntasks
      @no_active_ntasks = {}
      @no_active_ntasks_mutex = Mutex.new
      @no_active_ntasks_cv = ConditionVariable.new

      @pool_dict = PoolDictionary.new
    end

    attr_reader :id
    attr_reader :create_processor_mutex

    attr_reader :hash_seed

    def log_id
      "Controller[#{id}]"
    end

    def start(master_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Controller", self)

      @deepconnect.when_disconnected do |deepspace, opts|
	when_disconnected(deepspace, opts)
      end

      for name, obj in EXPORTS
	export(name, obj)
      end

      require "fairy/share/inspector"
      @deepconnect.export("Inspector", Inspector.new(self))

      require "fairy/share/log"
      @master_deepspace = @deepconnect.open_deepspace("localhost", master_port)
      @master = @master_deepspace.import("Master")
      @logger = @master.logger
      Log.type = "CONT"
      Log.pid = id
      Log.logger = @logger
      Log::info(self, "Controller Service Start")
      Log::info(self, "\tfairy version: #{Version}")
      Log::info(self, "\t[Powered by #{RUBY_DESCRIPTION}") 

      @master.register_controller(self)

    end

    def connect(client, conf)
      @client = client
      
      conf.base_conf = CONF
      Fairy::REPLACE_CONF(conf)
      
      mod = CONF.HASH_MODULE
      require mod
      @hash_seed = Fairy::HValueGenerator.create_seed
      def_pool_variable(:HASH_SEED, @hash_seed)

      @PROCESS_LIFE_MANAGE_INTERVAL = CONF.PROCESS_LIFE_MANAGE_INTERVAL

      if @PROCESS_LIFE_MANAGE_INTERVAL
	Thread.start do
	  start_process_life_manage
	end
	nil
      end

      $stdout = Stdout.new(@client)

    end
    DeepConnect.def_method_spec(self, "REF connext(REF, DVAL)")

    def terminate
      # clientが終了したときの終了処理
      # master から呼ばれる

Log::debug(self, "TERMINATE: #1")
# デッドロックするのでNG
#      @reserves_mutex.synchronize do
      @bjob2processors.keys.each do |bjob|
	begin
Log::debug(self, "TERMINATE: #1.1")
	  bjob.abort_create_node
	rescue
Log::debug(self, "TERMINATE: #1.1.1")
	  Log::debug_exception($!)
	end
      end
#      end

Log::debug(self, "TERMINATE: #2")
      cond = true
      while cond
Log::debug(self, "TERMINATE: #2.1")
	@reserves_mutex.synchronize do
Log::debug(self, "TERMINATE: #2.2")
	  cond = false if @reserves.empty?
	  @reserves.keys.each do |p|
Log::debug(self, "TERMINATE: #2.3")
	    if @reserves[p] == 0 
Log::debug(self, "TERMINATE: #2.4")
	      begin
		p.terminate_all_ntasks
	      rescue
Log::debug(self, "TERMINATE: #2.4.1")
		Log::debug_exception($!)
	      end
Log::debug(self, "TERMINATE: #2.5")
	      begin
		@reserves.delete(p)
	      rescue
Log::debug(self, "TERMINATE: #2.5.1")
		Log::debug_exception($!)
	      end
Log::debug(self, "TERMINATE: #2.5.1")
	      begin
Log::debug(self, "TERMINATE: #2.5.2")
		p.node.terminate_processor(p)
Log::debug(self, "TERMINATE: #2.5.3")
	      rescue
Log::debug(self, "TERMINATE: #2.5.4")
		Log::debug_exception($!)
	      end
Log::debug(self, "TERMINATE: #2.6")
	    end
	  end
Log::debug(self, "TERMINATE: #2.7")
	end
Log::debug(self, "TERMINATE: #2.8")
      end

Log::debug(self, "TERMINATE: #3")
      @reserves.keys.each do |p| 
	begin
	  p.node.terminate_processor(p)
	rescue
#	  p $!, $@
	end
      end

Log::debug(self, "TERMINATE: #4")
      Thread.start do
	sleep 0.2
	begin
	  @deepconnect.stop
	ensure
	  Process.exit!(0)
	end
      end
Log::debug(self, "TERMINATE: #5")
      nil
    end

    def terminate_rev0
      # clientが終了したときの終了処理
      # master から呼ばれる

Log::debug(self, "TERMINATE: #1")
      @reserves_mutex.synchronize do
	@bjob2processors.keys.each do |bjob|
	  bjob.abort_create_node
	end
      end

Log::debug(self, "TERMINATE: #2")
      @reserves.keys.each do |p| 
	begin
Log::debug(self, "TERMINATE: #2.1")
	  p.terminate_all_njobs
Log::debug(self, "TERMINATE: #2.2")
	rescue
	  LOG::debug_exception(self)
	end
      end

Log::debug(self, "TERMINATE: #2.5")
      @reserves.keys.each do |p| 
	begin
	  p.terminate_all_njobs
	rescue
	  LOG::debug_exception(self)
	end
      end

Log::debug(self, "TERMINATE: #3")
      @reserves.keys.each do |p| 
	begin
	  p.node.terminate_processor(p)
	rescue
#	  p $!, $@
	end
      end

Log::debug(self, "TERMINATE: #4")
      Thread.start do
	sleep 0.1
	@deepconnect.stop
	Process.exit(0)
      end
Log::debug(self, "TERMINATE: #5")
      nil
    end

    def when_disconnected(deepspace, opts)
      if deepspace == @client.deep_space
	Log::info(self, "CONTROLLER: disconnected: Start termination")
	# クライアントがおなくなりになったら, こっちも死ぬよ
	@master.terminate_controller(self)
      end
    end

    # 
    # clent interface
    #
    def export(service, obj)
      @services[service] = obj
    end

    def import(service)
      @services[service]
    end

    #
    # bjob methods
    #
    def register_bjob(bjob)
      @bjob2processors_mutex.synchronize do
	@bjob2processors[bjob] = []
      end
    end

    #
    # ntask methods
    #
    def no_active_ntasks_in_processor(processor)
      @no_active_ntasks_mutex.synchronize do
	@no_active_ntasks[processor] || 0
      end
    end

    def update_active_ntasks(processor, no_active_ntasks)
Log::debug(self, "Processor[#{processor.id}]" => #{no_active_ntasks}")
      @no_active_ntasks_mutex.synchronize do
	@no_active_ntasks[processor] = no_active_ntasks
	@no_active_ntasks_cv.broadcast
      end
    end

    #
    # processor methods
    #
    # reserve してから njob 割り当てを行う
    def reserve_processor(processor, &block)
      @reserves_mutex.synchronize do
	begin
	  return nil unless @reserves[processor]
	rescue DeepConnect::SessionServiceStopped
	  # processor は 終了している可能性がある
	  return nil
	end
	@reserves[processor] += 1
      end
      begin
	yield processor
	processor
      ensure
	@reserves_mutex.synchronize do
	  @reserves[processor] -= 1
	end
      end
    end

    def create_processor(node, bjob, &block)
      @create_processor_mutex.synchronize do
	processor = node.create_processor
	processor.connect_controller(self, CONF)
	@reserves_mutex.synchronize do
	  @reserves[processor] = 1
	end
	begin
	  register_processor(bjob, processor)
	  yield processor
	  processor
	ensure
	  @reserves_mutex.synchronize do
	    @reserves[processor] -= 1
	  end
	end
      end
    end

    def register_processor(bjob, processor)
      @bjob2processors_mutex.synchronize do
	@bjob2processors[bjob] = [] unless @bjob2processors[bjob]
	unless @bjob2processors[bjob].include?(processor)
	  @bjob2processors[bjob].push processor
	end
	@bjob2processors_cv.broadcast
      end
      processor
    end

#     def assign_inputtable_processor(bjob, input_bjob, input_njob, input_export, &block)
#       case input_bjob
#       when BGroupBy
# 	assign_processor(bjob, :NEW_PROCESSOR_N, input_bjob, &block)
# #	assign_processor(bjob, :NEW_PROCESSOR, &block)
# #	assign_processor(bjob, :SAME_PROCESSOR, input_njob.processor, &block)
#       when BSplitter
# 	assign_processor(bjob, :NEW_PROCESSOR, &block)
# #	assign_processor(bjob, :NEW_PROCESSOR_N, input_bjob, &block)
# #	assign_processor(bjob, :SAME_PROCESSOR, input_njob.processor, &block)
#       else
# 	assign_processor(bjob, :SAME_PROCESSOR, input_njob.processor, &block)
#       end
#     end

#     # Processor 関連メソッド
#     # Policy: :SAME_PROCESSOR, :NEW_PROCESSOR, :INPUT, MUST_BE_SAME_PROCESSOR
#     def assign_processor(bjob, policy, *opts, &block)
#       case policy
#       when :INPUT
# 	assign_input_processor(bjob, opts[0], &block)
#       when :SAME_PROCESSOR_OBJ
# 	assign_same_obj_processor(bjob, opts[0], &block)
#       when :SAME_PROCESSOR, :MUST_BE_SAME_PROCESSOR
# 	processor = opts[0]
# 	assign_same_processor(bjob, processor, &block)
#       when :NEW_PROCESSOR
# 	assign_new_processor(bjob, &block)
#       when :NEW_PROCESSOR_N
# 	input_bjob = opts[0]
# 	assign_new_processor_n(bjob, input_bjob, &block)
#       else
# 	ERR::Raise ERR::INTERNAL::UndefinedPolicy, policy.to_s
#       end
#     end

    #
    # methods of assgin processor.
    #
    def assign_input_processor(bjob, host, &block)
      node = @master.node_in_reisured(host)
      unless node
	ERR::Raise ERR::NodeNotArrived, host
      end
      create_processor(node, bjob, &block)
    end

    def assign_input_processor_n(bjob, host, &block)
      node = @master.node_in_reisured(host)
      ERR::Raise ERR::NodeNotArrived, host unless node

      max_no = CONF.CONTROLLER_INPUT_PROCESSOR_N
      max_tasks = CONF.CONTROLLER_MAX_ACTIVE_NTASKS_IN_PROCESSOR

      loop do
	no_of_processors = 0
	leisured_processor = nil
	min = nil
	for processor in @bjob2processors[bjob].dup
	  next if processor.node != node
	  no_of_processors += 1
	  
	  n = no_active_ntasks_in_processor(processor)
	  if !min or min > n
	    min = n
	    leisured_processor = processor
	  end
	end

	if max_no.nil? || max_no >= no_of_processors
	  create_processor(node, bjob, &block)
	  break
	elsif min > max_tasks
	  @no_active_ntasks_mutex.synchronize do
	    Log::debug(self, "NO_ACTIVE_NTASKS: WAIT")
	    @no_active_ntasks_cv.wait(@no_active_ntasks_mutex)
	    Log::debug(self, "NO_ACTIVE_NTASKS: WAIT END")
	  end
	else
	  ret = reserve_processor(leisured_processor) {|processor|
	    register_processor(bjob, processor)
	    yield processor
	  }
	  unless ret
	    # プロセッサが終了していたとき. もうちょっとどうにかしたい気もする
	    assign_new_processor(bjob, &block)
	  end
	  break
	end
      end
    end

#     def assign_input_processor_n(bjob, host, &block)
# Log::debug(self, "HHHHHHHHHHHHHHHHHHHHH: #{host}")
#       no = 0
#       if processors = @bjob2processors[bjob]
# 	no += processors.size
#       end

#       max_no = CONF.CONTROLLER_INPUT_PROCESSOR_N
#       if max_no.nil? || max_no > no
#         node = @master.node_in_reisured(host)
# 	ERR::Raise ERR::NodeNotArrived, host unless node

# 	create_processor(node, bjob, &block)
#       else
#         node = @master.node_in_reisured(host)
# 	ERR::Raise ERR::NodeNotArrived, host unless node

# 	leisured_processor = nil
# 	min = nil
# 	for processor in @bjob2processors[bjob].dup
#           next if processor.node != node
	  
# 	  n = processor.no_ntasks
# 	  if !min or min > n
# 	    min = n
# 	    leisured_processor = processor
# 	  end
# 	end
# 	ret = reserve_processor(leisured_processor) {|processor|
# 	  register_processor(bjob, processor)
# 	  yield processor
# 	}
# 	unless ret
# 	  # プロセッサが終了していたとき. もうちょっとどうにかしたい気もする
# 	  assign_new_processor(bjob, &block)
# 	end
#       end
#     end


    def assign_same_processor(bjob, processor, &block)
      # このメソッドは, 基本的にはreserve しているだけ
      ret = reserve_processor(processor) {|processor|
	register_processor(bjob, processor)
	yield processor
	processor}

      unless ret
	# プロセッサが終了していたとき(ほとんどあり得ないけど)
	# この時のassgin_processor側の処理がイマイチ
	assign_new_processor(bjob, &block)
      end
    end

    def assign_same_obj_processor(bjob, obj, &block)
      processor = nil
      @reserves_mutex.synchronize do
	@reserves.each_key do |p| 
	  if p.deep_space == obj.deep_space
	    processor = p
	    break
	  end
	end
      end
      ERR::Raise ERR::NoExistProcesorWithObject obj.to_s unless processor

      ret = reserve_processor(processor) {
	register_processor(bjob, processor)
	yield processor
      }
      
      ERR::Raise ERR::NoExistProcesorWithObject obj.to_s unless ret
    end

    def assign_new_processor(bjob, &block)
      node = @master.leisured_node
      create_processor(node, bjob, &block)
    end

    # まあ, 大体n個になるかなぁ... 
    # input_bjobのプロセスも動的に割り当てられるので...
    # 最終的には 大体そうなるということで....
    def assign_new_processor_n(bjob, input_bjob, &block)

      if input_bjob
	no_i = 0
	@bjob2processors_mutex.synchronize do
	  while !@bjob2processors[input_bjob]
	    @bjob2processors_cv.wait(@bjob2processors_mutex)
	  end
	  if i_processors = @bjob2processors[input_bjob]
	    no_i += i_processors.size
	  end
	end
	max_no = no_i * CONF.CONTROLLER_ASSIGN_NEW_PROCESSOR_N_FACTOR
      else
	# ここバグっている. CONTROLLER_INPUT_PROCESSOR_N は1-node辺りの数
	max_no = CONF.CONTROLLER_INPUT_PROCESSOR_N
      end

      max_tasks = CONF.CONTROLLER_MAX_ACTIVE_NTASKS_IN_PROCESSOR

      loop do
	no = 0
	if processors = @bjob2processors[bjob]
	  no += processors.size
	end

	if max_no > no
	  node = @master.leisured_node
	  create_processor(node, bjob, &block)
	else
	  leisured_processor = nil
	  min = nil
	  for processor in @bjob2processors[bjob].dup
	    # これだと頭から割り当てられる... 
	    # けど取りあえずということで.
	    
	    n = no_active_ntasks_in_processor(processor)
	    if !min or min > n
	      min = n
	      leisured_processor = processor
	    end
	  end

	  if min > max_ntasks
	    @no_active_ntasks_mutex.synchronize do
	      Log::debug(self, "NO_ACTIVE_NTASKS: WAIT")
	      @no_active_ntasks_cv.wait(@no_active_ntasks_mutex)
	      Log::debug(self, "NO_ACTIVE_NTASKS: WAIT END")
	    end
	  else
	    ret = reserve_processor(leisured_processor) {|processor|
	      register_processor(bjob, processor)
	      yield processor
	    }
	    unless ret
	      # プロセッサが終了していたとき. もうちょっとどうにかしたい気もする
	      assign_new_processor(bjob, &block)
	    end
	    break
	  end
	end
      end
    end

    def assign_new_processor_n_for_local_io(bjob, &block)

      nodes = {}
#      for p in @bjob2processors[bjob].dup
      for p in bjob.nodes.collect{|njob| njob.processor}
	if nodes[p.node]
	  nodes[p.node].push p
	else
	  nodes[p.node] = [p]
	end
      end

      node = nil
      assign_level = 0
      while !node
	assign_level += 1
	except_nodes = nodes.select{|n, ps| ps.size >= assign_level}
	node = @master.leisured_node_except_nodes(except_nodes, false)
      end

      max_no = CONF.CONTROLLER_INPUT_PROCESSOR_N
      if nodes[node]
	leisured_processor = nil
	min = nil
	for processor in nodes[node]
	  n = processor.no_ntasks
	  if !min or min > n
	    min = n
	    leisured_processor = processor
	  end
	end
	no_of_processors = nodes[node].size
      else
	no_of_processors = 0
      end

      if max_no.nil? || max_no >= no_of_processors
	create_processor(node, bjob, &block)
      else
	ret = reserve_processor(leisured_processor) {|processor|
	  register_processor(bjob, processor)
	  yield processor
	}
	unless ret
	  # プロセッサが終了していたとき. もうちょっとどうにかしたい気もする
	  assign_new_processor(bjob, &block)
	end
      end
    end

    def terminate_processor
      deresister_processor(processor)
      @master.deregister_processor(processor)
      @node.deregister_processor(processor)
      @node.terminate_processor
    end

    def start_process_life_manage
      loop do
	sleep PROCESS_LIFE_MANAGE_INTERVAL
	Log::debug(self, "START_PROCESS_LIFE_MANAGE: S")
	processors = @reserves_mutex.synchronize{@reserves.keys}
	for p in processors
Log::debug(self, "START_PROCESS_LIFE_MANAGE: 1 %{p}")
	  kill = false
	  @reserves_mutex.synchronize do
#  	    for q, r in @reserves
#  	      puts "#{q.id} =>#{r}"
#  	    end
Log::debug(self, "START_PROCESS_LIFE_MANAGE: 2 ")
	    if @reserves[p] == 0 && p.life_out_life_span?
	      Log::info self, "Kill #{p.inspectx}"
	      kill = true
	      @reserves.delete(p)
	      @bjob2processors_mutex.synchronize do
		# @bjob2processors から p を削除する必要あるか?
	      end
	    end
	  end
 Log::debug(self, "START_PROCESS_LIFE_MANAGE: 3 ")
	  if kill
 Log::debug(self, "START_PROCESS_LIFE_MANAGE: 4 ")
	    p.node.terminate_processor(p)
	  end
 Log::debug(self, "START_PROCESS_LIFE_MANAGE: 5 ")
	end
 Log::debug(self, "START_PROCESS_LIFE_MANAGE: E ")
      end
    end

    # exception handling
    def handle_exception(exp)
      Thread.start do
	begin
	  @client.handle_exception(exp)
	rescue
	end
      end
      nil
    end

    # stdout
    def stdout_write(str)
      $stdout.replace_stdout do
	$stdout.write(str)
      end
    end

    # pool variable
    def pool_dict
      @pool_dict
    end

    def def_pool_variable(vname, value = nil)
      # value が Hash で キー :block をもっていたら block と見なす.
      if value.__deep_connect_reference? && value.kind_of?(Hash) && value[:block]
	p = Context.create_proc(self, value[:block])
	value = p.call 
      end
      @pool_dict.def_variable(vname, value)
    end

    def pool_variable(vname, *value)
      if value.empty?
	@pool_dict[vname]
      else
	@pool_dict[vname] = value.first
      end
    end

    class Context
      def self.create_proc(controller, source)
	context = new(controller)
	context.create_proc(source)
      end
      
      def initialize(controller)
	@Pool = controller.pool_dict
      end

      def create_proc(source)
	eval("proc{#{source}}", binding)
      end
    end

    #-- new fairy


#     def assign_processor(target_bjob, &block)
#       mapper = NjobMapper.new(self, target_bjob)
#       mapper.assign_processor(&block)
#     end

    def assign_ntasks(target_bjob, create_node_mutex, &block)
      target_bjob.input.each_assigned_filter do |input_filter|
	mapper = NjobMapper.new(self, target_bjob, input_filter)
#	create_node_mutex.synchronize do
	  mapper.assign_ntask(&block)
#	end
      end
    end

    class NjobMapper

      def initialize(cont, target_bjob, input_filter)
	@controller = cont
	@target_bjob = target_bjob

	@pre_bjob = @target_bjob.input
	@input_filter = input_filter

	init_policy

	Log::debug(self, "Mapping Policy: #{@pre_bjob.class} -(#{@policy.class})-> #{@target_bjob.class}")
	
      end

      attr_reader :controller
      attr_reader :pre_bjob
      attr_reader :target_bjob
      attr_reader :input_filter
      attr_reader :policy

      def init_policy
	if @pre_bjob.respond_to?(:postmapping_policy) && 
	    @pre_bjob.postmapping_policy
	  @policy = eval(@pre_bjob.postmapping_policy.to_s).new(self)
	  return
	end

# 今のところは必要なし(lazy create njob時に対応)
#  	if @target_bjob.kind_of?(BShuffle)
#  	  @policy = MPNewProcessorN.new(self)
#  	  return
#  	end

	case @pre_bjob
	when BFilePlace
	  #BInput系
	  @policy = MPInputProcessor.new(self)
	when BLocalIOPlace
	  @policy = MPLocalInputNewProcessorN.new(self)
	when BIotaPlace, BTherePlace
	  @policy = MPInputNewProcessorN.new(self)
	when BVarrayPlace
	  @policy = MPVarrayInputProcessor.new(self)
#	when BIotaPlace
#	  @policy = MPIotaInputProcessor.new(self)
	when BGroupBy, BDirectProduct::BPreFilter #, BShuffle 
	  @policy = MPNewProcessorN.new(self)
#	  @policy = MPNewProcessor.new(self)
	when BSplitter, BInject::BLocalInject, BFind::BLocalFind
	  @policy = MPNewProcessor.new(self)
#	when BShuffle
#	  @policy = MPPostShuffle.new(self)
#	when BZipper::BPreZippedFilter
#	  @policy = MPZippedFilter.new(self)
	else
	  @policy = MPSameNTask.new(self)
	end
      end

      def assign_ntask(&block)
	@policy.assign_ntask(&block)
      end

      def bind_input(njob)
	@policy.bind_input(njob)
      end
    end

    class NjobMappingPolicy
      extend Forwardable

      def initialize(mapper)
	@mapper = mapper
      end

      def_delegator :@mapper, :controller
      def_delegator :@mapper, :pre_bjob
      def_delegator :@mapper, :target_bjob
      def_delegator :@mapper, :input_filter

    end

    class MPInputProcessor < NjobMappingPolicy
      def assign_ntask(&block)
	controller.assign_input_processor_n(target_bjob, 
					  input_filter.host) do |processor|
	  ntask = processor.create_ntask
	  block.call(ntask, @mapper)
	end
      end

      def bind_input(njob)
	njob.open(input_filter)
      end
    end

    class MPInputNewProcessor< MPInputProcessor
      def assign_ntask(&block)
	controller.assign_new_processor(target_bjob) do |processor|
	  ntask = processor.create_ntask
	  block.call(ntask, @mapper)
	end
      end
    end

    class MPInputNewProcessorN< MPInputProcessor
      def assign_ntask(&block)
	controller.assign_new_processor_n(target_bjob,
					  nil) do |processor|
	  ntask = processor.create_ntask
	  block.call(ntask, @mapper)
	end
      end
    end

    class MPLocalInputNewProcessorN< MPInputProcessor
      def assign_ntask(&block)
	controller.assign_new_processor_n_for_local_io(target_bjob) do |processor|
	  ntask = processor.create_ntask
	  block.call(ntask, @mapper)
	end
      end
    end

    class MPVarrayInputProcessor < MPInputProcessor
      def assign_ntask(&block)
	controller.assign_same_obj_processor(target_bjob, 
					     input_filter.ary) do |processor|
	  ntask = processor.create_ntask
	  block.call(ntask, @mapper)
	end
      end
    end

    class MPSameNTask < NjobMappingPolicy
      def initialze(mapper)
	super
	@import = nil
      end

      def assign_ntask(&block)
	# thread を立ち上げるべき
	# このままでは, 十分に並列性が取れない(for [REQ:#5)]
	controller.assign_same_processor(target_bjob, 
					 input_filter.processor) do |processor|
	  ntask = input_filter.ntask
	  if input_filter.processor != processor
	    Log::warn(self, "ASSIGN_NTASK: assign defferent processor!!")
	    ntask = processor.create_ntask
	  end
	  block.call(ntask, @mapper)
	end
      end

      def bind_input(njob)
	njob.input = input_filter
      end
    end


# 必要ない?
#    class MPSameProcessorObj < NjobMappingPolicy
#    end

    class MPNewProcessor < NjobMappingPolicy
      
      def initialize(mapper)
	super
	@export = nil
	@import = nil
      end

      def assign_ntask(&block)
	pre_bjob.start_export(input_filter)

	pre_bjob.each_export_by(input_filter, self) do |export, opts={}|
#	pre_bjob.each_export_by(input_filter, self) do |export, opts|
#	  opts = {} unless opts

	  # thread を立ち上げるべき
	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
	  controller.assign_new_processor(target_bjob) do |processor|
	    # シリアライズに処理されることが前提になっている
	    @export = export
	    @import = target_bjob.create_import(processor)
	    ntask = processor.create_ntask
	    block.call(ntask, @mapper, opts)
	  end
	end
      end

      def bind_input(njob)
	@import.no = @export.no
	@import.key = @export.key
	njob.input = @import
	@export.output = @import
	pre_bjob.bind_export(@export, @import)
      end
    end

    class MPNewProcessorN < MPNewProcessor
      def assign_ntask(&block)
	pre_bjob.start_export(input_filter)

	pre_bjob.each_export_by(input_filter, self) do |export, opts={}|
#	pre_bjob.each_export_by(input_filter, self) do |export, opts|
#	  opts = {} unless opts
	  # thread を立ち上げるべき
	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
	  controller.assign_new_processor_n(target_bjob, pre_bjob) do 
	    |processor|
	    # シリアライズに処理されることが前提になっている
	    @export = export
	    @import = target_bjob.create_import(processor)
	    ntask = processor.create_ntask
	    block.call(ntask, @mapper, opts)
	  end
	end
      end
    end

    class MPSameProcessor < MPNewProcessor
      
      def assign_ntask(&block)
	pre_bjob.start_export(input_filter)

	pre_bjob.each_export_by(input_filter, self) do |export, opts={}|
#	pre_bjob.each_export_by(input_filter, self) do |export, opts|
# Log::debug(self, "YYYYYYYYYYYYYYY: #{export.class}, #{opts.class}")
#	  opts = {} unless opts
	  # thread を立ち上げるべき
	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
	  controller.assign_same_processor(target_bjob,
					   input_filter.processor) do
	    |processor|
	    # シリアライズに処理されることが前提になっている
	    @export = export
	    @import = target_bjob.create_import(processor)

	    ntask = processor.create_ntask
	    block.call(ntask, @mapper, opts)
	  end
	end
      end
    end
    MPSameProcessorQ = MPSameProcessor

#     class MPZippedFilter<MPNewProcessor
      
#       def assign_processor(&block)
# 	pre_bjob.start_export(input_filter)

# 	pre_bjob.each_export_by(input_filter, self) do |export|
# 	  # thread を立ち上げるべき
# 	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
# 	  controller.assign_new_processor(target_bjob) do |processor|
# 	    # シリアライズに処理されることが前提になっている
# 	    @export = export
# 	    @import = target_bjob.create_import(processor)
# 	    block.call(processor, @mapper)
# 	  end
# 	end
#       end

#       def bind_input(njob)
# 	@import.no = @export.no
# 	@import.key = @export.key
# 	njob.input = @import
# 	@export.output = @import
# 	pre_bjob.bind_export(@export, @import)
#       end
#     end


#     class MPPreShuffle < NjobMappingPolicy
#       def each_exports(&block)
# 	pre_bjob.start_export(input_filter)

# 	pre_bjob.each_export_by(input_filter, self) do |export|
# 	  # thread を立ち上げるべき
# 	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
# 	  @export = export
# 	  block.call(export)
# 	end
#       end
#     end

#     class MPPostShuffle < MPNewProcessorN
#       def assign_processor(&block)
# 	# すでにスタートしている
# 	#pre_bjob.start_export(input_filter)

# 	pre_bjob.each_export_by(input_filter, self) do |export|
# 	  # thread を立ち上げるべき
# 	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
# 	  controller.assign_new_processor_n(target_bjob, pre_bjob) do 
# 	    |processor|
# 	    # シリアライズに処理されることが前提になっている
# 	    @export = export
# 	    @import = target_bjob.create_import(processor)
# 	    block.call(processor, @mapper)
# 	  end
# 	end
#       end
#     end
  end
end

require "fairy/backend/addins"
