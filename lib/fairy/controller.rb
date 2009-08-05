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

      # processor -> no of reserve 
      @reserves = {}
      @reserves_mutex = Mutex.new
      @reserves_cv = ConditionVariable.new

      # bjob -> [processor, ...]
      @bjob2processors = {}
      @bjob2processors_mutex = Mutex.new
      @bjob2processors_cv = ConditionVariable.new

      @pool_dict = PoolDictionary.new

      mod = CONF.HASH_MODULE
      require mod
      @hash_seed = Fairy::HValueGenerator.create_seed
    end

    attr_reader :id
    attr_reader :hash_seed

    PROCESS_LIFE_MANAGE_INTERVAL = CONF.PROCESS_LIFE_MANAGE_INTERVAL

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
      Log::info(self, "\tRuby version: #{RUBY_VERSION}") 

      @master.register_controller(self)

      if PROCESS_LIFE_MANAGE_INTERVAL
	Thread.start do
	  start_process_life_manage
	end
	nil
      end
    end

    def connect(client)
      @client = client
      
      $stdout = Stdout.new(@client)
    end

    def terminate
      # clientが終了したときの終了処理
      # master から呼ばれる

#Log::debug(self, "TERMINATE: #1")
# デッドロックするのでNG
#       @reserves_mutex.synchronize do
# 	@bjob2processors.keys.each do |bjob|
# 	  bjob.abort_create_node
# 	end
#       end

#Log::debug(self, "TERMINATE: #2")
      cond = true
      while cond
#Log::debug(self, "TERMINATE: #2.1")
	@reserves_mutex.synchronize do
#Log::debug(self, "TERMINATE: #2.2")
	  cond = false if @reserves.empty?
	  @reserves.keys.each do |p|
#Log::debug(self, "TERMINATE: #2.3")
	    if @reserves[p] == 0 
#Log::debug(self, "TERMINATE: #2.4")
	      p.terminate_all_njobs
	      @reserves.delete(p)
	      p.node.terminate_processor(p)
	    end
	  end
	end
      end

#Log::debug(self, "TERMINATE: #3")
      @reserves.keys.each do |p| 
	begin
	  p.node.terminate_processor(p)
	rescue
#	  p $!, $@
	end
      end

#Log::debug(self, "TERMINATE: #4")
      Thread.start do
	sleep 0.1
	@deepconnect.stop
	Process.exit!(0)
      end
#Log::debug(self, "TERMINATE: #5")
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
      processor = node.create_processor
      processor.set_stdout(self)
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
      node = @master.node(host)
      unless node
	ERR::Raise ERR::NodeNotArrived, host
      end
      create_processor(node, bjob, &block)
    end


    def assign_same_processor(bjob, processor, &block)
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
	  
	  n = processor.no_njobs
	  if !min or min > n
	    min = n
	    leisured_processor = processor
	  end
	end
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
	processors = @reserves_mutex.synchronize{@reserves.keys}
	for p in processors
	  kill = false
	  @reserves_mutex.synchronize do
#  	    for q, r in @reserves
#  	      puts "#{q.id} =>#{r}"
#  	    end
	    if @reserves[p] == 0 && p.life_out_life_span?
	      Log::info self, "Kill #{p.inspectx}"
	      kill = true
	      @reserves.delete(p)
	      @bjob2processors_mutex.synchronize do
		# @bjob2processors から p を削除する必要あるか?
	      end
	    end
	  end
	  if kill
	    p.node.terminate_processor(p)
	  end
	end
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

    def assign_processors(target_bjob, &block)
      target_bjob.input.each_assigned_filter do |input_filter|
	mapper = NjobMapper.new(self, target_bjob, input_filter)
	mapper.assign_processor(&block)
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
	  @policy = eval(@pre_bjob.postmapping_policy).new(self)
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
	when BLocalIOPlace, BIotaPlace, BTherePlace
	  @policy = MPInputNewProcessor.new(self)
	when BVarrayPlace
	  @policy = MPVarrayInputProcessor.new(self)
	when BIotaPlace
	  @policy = MPIotaInputProcessor.new(self)
	when BGroupBy #, BShuffle
	  @policy = MPNewProcessorN.new(self)
#	  @policy = MPNewProcessor.new(self)
	when BSplitter
	  @policy = MPNewProcessor.new(self)
#	when BShuffle
#	  @policy = MPPostShuffle.new(self)
#	when BZipper::BPreZippedFilter
#	  @policy = MPZippedFilter.new(self)
	else
	  @policy = MPSameProcessor.new(self)
	end
      end

      def assign_processor(&block)
	@policy.assign_processor(&block)
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
      def assign_processor(&block)
	controller.assign_input_processor(target_bjob, 
					  input_filter.host) do |processor|
	  block.call(processor, @mapper)
	end
      end

      def bind_input(njob)
	njob.open(input_filter)
      end
    end

    class MPInputNewProcessor< MPInputProcessor
      def assign_processor(&block)
	controller.assign_new_processor(target_bjob) do |processor|
	  block.call(processor, @mapper)
	end
      end
    end

    class MPVarrayInputProcessor < MPInputProcessor
      def assign_processor(&block)
	controller.assign_same_obj_processor(target_bjob, 
					     input_filter.ary) do |processor|
	  block.call(processor, @mapper)
	end
      end
    end

    class MPSameProcessor < NjobMappingPolicy
      def initialze(mapper)
	super
	@import = nil
      end

      def assign_processor(&block)
	# thread を立ち上げるべき
	# このままでは, 十分に並列性が取れない(for [REQ:#5)]
	controller.assign_same_processor(target_bjob, 
					 input_filter.processor) do |processor|
	  block.call(processor, @mapper)
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

      def assign_processor(&block)
	pre_bjob.start_export(input_filter)

	pre_bjob.each_export_by(input_filter, self) do |export|
	  # thread を立ち上げるべき
	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
	  controller.assign_new_processor(target_bjob) do |processor|
	    # シリアライズに処理されることが前提になっている
	    @export = export
	    @import = target_bjob.create_import(processor)
	    block.call(processor, @mapper)
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
      def assign_processor(&block)
	pre_bjob.start_export(input_filter)

	pre_bjob.each_export_by(input_filter, self) do |export|
	  # thread を立ち上げるべき
	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
	  controller.assign_new_processor_n(target_bjob, pre_bjob) do 
	    |processor|
	    # シリアライズに処理されることが前提になっている
	    @export = export
	    @import = target_bjob.create_import(processor)
	    block.call(processor, @mapper)
	  end
	end
      end
    end

    class MPSameProcessorQ < MPNewProcessor
      
      def assign_processor(&block)
	pre_bjob.start_export(input_filter)

	pre_bjob.each_export_by(input_filter, self) do |export|
	  # thread を立ち上げるべき
	  # このままでは, 十分に並列性が取れない(for [REQ:#5)]
	  controller.assign_same_processor(target_bjob,
					   input_filter.processor) do |processor|
	    # シリアライズに処理されることが前提になっている
	    @export = export
	    @import = target_bjob.create_import(processor)
	    block.call(processor, @mapper)
	  end
	end
      end
    end


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
