
require "thread"

require "deep-connect/deep-connect.rb"
#DeepConnect::Organizer.immutable_classes.push Array

#require "backend/job-interpriter"
#require "backend/scheduler"

require "share/pool-dictionary"
require "share/stdout"
require "share/log"

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
	  raise "クラス以外を登録するときにはサービス名が必要です(%{obj})"
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

    end

    attr_reader :id

#    PROCESS_LIFE_MANAGE_INTERVAL = 60
#    PROCESS_LIFE_MANAGE_INTERVAL = 10
#    PROCESS_LIFE_MANAGE_INTERVAL = 1
    PROCESS_LIFE_MANAGE_INTERVAL = nil

    def start(master_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Controller", self)

      @deepconnect.when_disconnected do |deepspace, opts|
	when_disconnected(deepspace, opts)
      end

      for name, obj in EXPORTS
	export(name, obj)
      end

      @master_deepspace = @deepconnect.open_deepspace("localhost", master_port)
      @master = @master_deepspace.import("Master")
      @logger = @master.logger
      Log.type = "CONT"
      Log.logger = @logger
      Log::info(self, "Controller Service Start")

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
      processors = @reserves.keys
      processors.each do |p| 
	begin
	  p.node.terminate_processor(p)
	rescue
#	  p $!, $@
	end
      end

      Thread.start do
	sleep 0.1
	@deepconnect.stop
	Process.exit(0)
      end
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
	rescue SessionServiceStopped
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

    def assign_inputtable_processor(bjob, input_bjob, input_njob, input_export, &block)
      case input_bjob
      when BGroupBy
	assign_processor(bjob, :NEW_PROCESSOR_N, input_bjob, &block)
#	assign_processor(bjob, :NEW_PROCESSOR, &block)
      when BSplitter
	assign_processor(bjob, :NEW_PROCESSOR, &block)
      else
	assign_processor(bjob, :SAME_PROCESSOR, input_njob.processor, &block)
      end
    end

    # Processor 関連メソッド
    # Policy: :SAME_PROCESSOR, :NEW_PROCESSOR, :INPUT, MUST_BE_SAME_PROCESSOR
    def assign_processor(bjob, policy, *opts, &block)
      case policy
      when :INPUT
	assign_input_processor(bjob, opts[0], &block)
      when :SAME_PROCESSOR_OBJ
	assign_same_obj_processor(bjob, opts[0], &block)
      when :SAME_PROCESSOR, :MUST_BE_SAME_PROCESSOR
	processor = opts[0]
	assign_same_processor(bjob, processor, &block)
      when :NEW_PROCESSOR
	assign_new_processor(bjob, &block)
      when :NEW_PROCESSOR_N
	input_bjob = opts[0]
	assign_new_processor_n(bjob, input_bjob, &block)
      else
	raise "未サポートのポリシー: #{policy}"
      end
    end

    def assign_input_processor(bjob, host, &block)
      node = @master.node(host)
      unless node
	raise "#{host} のホスト上でnodeが立ち上がっていません"
      end

      create_processor(node, bjob, &block)
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
      raise "#{obj} の存在するプロセッサは存在しません" unless processor

      ret = reserve_processor(processor) {
	register_processor(bjob, processor)
	yield processor
      }
      
      raise "#{obj} の存在するプロセッサは存在しません" unless ret
    end

    def assign_same_processor(bjob, processor, &block)
      ret = reserve_processor(processor) {|processor|
	register_processor(bjob, processor)
	yield processor
	processor}

      unless ret
	# プロセッサが終了していたとき(ほとんどあり得ないけど)
	assign_new_processor(bjob, &block)
      end
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
	  no_i = i_processors.size
	end
      end

      no = 0
      if processors = @bjob2processors[bjob]
	no = processors.size
      end
      if no_i > no
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
	      Log::debug self, "Kill #{p.inspectx}"
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

  end
end

require "backend/addins"
