
require "deep-connect/deep-connect.rb"

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
	  raise "���饹�ʳ�����Ͽ����Ȥ��ˤϥ����ӥ�̾��ɬ�פǤ�(%{obj})"
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

    def create_processor(node, bjob)
      processor = node.create_processor
      @master.register_processor(node, processor)
      register_processor(bjob, processor)
      processor
    end

    def assign_inputtable_processor(bjob, input_bjob, input_njob, input_export)
      case input_bjob
      when BGroupBy
	assign_processor(bjob, :NEW_PROCESSOR_N, input_bjob)
#	assign_processor(bjob, :NEW_PROCESSOR)
      when BSplitter
	assign_processor(bjob, :NEW_PROCESSOR)
      else
	assign_processor(bjob, :SAME_PROCESSOR, input_njob.processor)
      end
    end

    # Processor ��Ϣ�᥽�å�
    # Policy: :SAME_PROCESSOR, :NEW_PROCESSOR, :INPUT, MUST_BE_SAME_PROCESSOR
    def assign_processor(bjob, policy, *opts)
      case policy
      when :INPUT
	assign_input_processor(bjob, opts[0])
      when :SAME_PROCESSOR, :MUST_BE_SAME_PROCESSOR
	processor = opts[0]
	assign_same_processor(bjob, processor)
      when :NEW_PROCESSOR
	assign_new_processor(bjob)
      when :NEW_PROCESSOR_N
	input_bjob = opts[0]
	assign_new_processor_n(bjob, input_bjob)
      else
	raise "̤���ݡ��ȤΥݥꥷ��: #{policy}"
      end
    end

    def assign_input_processor(bjob, host)
      node = @master.node(host)
      unless node
	raise "#{host} �Υۥ��Ⱦ��node��Ω���夬�äƤ��ޤ���"
      end

      create_processor(node, bjob)
    end
    
    def assign_same_processor(bjob, processor)
      register_processor(bjob, processor)
    end


    def assign_new_processor(bjob)
      node = @master.leisured_node
      create_processor(node, bjob)
    end

    # �ޤ�, ����n�Ĥˤʤ뤫�ʤ�... 
    # input_bjob�Υץ�����ưŪ�˳�����Ƥ���Τ�...
    # �ǽ�Ū�ˤ� ���Τ����ʤ�Ȥ������Ȥ�....
    def assign_new_processor_n(bjob, input_bjob)
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
	create_processor(node, bjob)
      else
	leisured_processor = nil
	min = nil
	for processor in @bjob2processors[bjob].dup
	  # �������Ƭ���������Ƥ���... 
	  # ���ɼ�ꤢ�����Ȥ������Ȥ�.
	  n = processor.no_njobs
	  if !min or min > n
	    min = n
	    leisured_processor = processor
	  end
	end
	register_processor(bjob, leisured_processor)
	leisured_processor
      end
    end

    # pool variable

    def pool_dict
      @pool_dict
    end

    def def_pool_variable(vname, value = nil)
      @pool_dict.def_variable(vname, value)
    end
    # ����ä�Ǻ�ޤ�������, VAL��̵��?
    DeepConnect.def_method_spec(self, :method=>:def_pool_variable, :args=>["VAL", "DVAL"])

    def pool_variable(vname, *value)
      if value.empty?
	@pool_dict[vname]
      else
	@pool_dict[vname] = value
      end
    end
  end
end

require "backend/addins"
