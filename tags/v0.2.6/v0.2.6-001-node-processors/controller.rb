
require "deep-connect/deep-connect.rb"
#DeepConnect::Organizer.immutable_classes.push Array

#require "backend/job-interpriter"
#require "backend/scheduler"

require "share/pool-dictionary"


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

      @services = {}

      # bjob -> [processor, ...]
      @bjob2processors = {}
      @bjob2processors_mutex = Mutex.new
      @bjob2processors_cv = ConditionVariable.new

      @pool_dict = PoolDictionary.new
    end

    attr_reader :id

    def start(master_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.export("Controller", self)

      for name, obj in EXPORTS
	export(name, obj)
      end

      @master_deepspace = @deepconnect.open_deepspace("localhost", master_port)
      @master = @master_deepspace.import("Master")
      @master.register_controller(self)
    end

    def export(service, obj)
      @services[service] = obj
    end

    def import(service)
      @services[service]
    end

    #
    # processor methods
    #
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

    def create_processor(node, bjob, &block)
      node.create_processor do |processor|
	@master.register_processor(node, processor)
	register_processor(bjob, processor)
	yield processor
	processor
      end
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
      node = @master.node(obj.deep_space.peer_uuid[0])
      unless node
	raise "#{obj} の存在するホスト上でnodeが立ち上がっていません"
      end

      node.reserve_processor_with_uuid(obj.deep_space.peer_uuid) do |processor|
	register_processor(bjob, processor)
	yield processor
      end
    end

    def assign_same_processor(bjob, processor, &block)
      ret = processor.node.reserve_processor(processor) {|processor|
	register_processor(bjob, processor)
	yield processor}

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
      end
      if i_processors = @bjob2processors[input_bjob]
	no_i = i_processors.size
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
	ret = leisured_processor.node.reserve_processor(leisured_processor) {|processor|
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
