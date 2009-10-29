# encoding: UTF-8

require "deep-connect/deep-connect"

require "fairy/version"
require "fairy/share/conf"
require "fairy/share/stdout"

#DeepConnect::Organizer.immutable_classes.push Array

# require "node/nfile"
# require "node/n-local-file-input"
# require "node/n-input-iota"
# require "node/n-there"

# require "node/n-file-output"
# require "node/n-local-file-output"

# require "node/nhere"
# require "node/n-each-element-mapper"
# require "node/n-each-element-selector"
# require "node/n-each-substream-mapper"
# require "node/n-group-by"
# require "node/n-zipper"
# require "node/n-splitter"
# require "node/n-barrier"

module Fairy

  class Processor

    EXPORTS = []
    def Processor.def_export(obj, name = nil)
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

    def initialize(id)
      @id = id
      @reserve = 0

      @services = {}

      @ntasks = []

      init_varray_feature
      init_ntask_status_feature
    end

    attr_reader :id
    attr_reader :ntasks

    def start(node_port, service=0)
#      if CONF.THREAD_STACK_SIZE
#	Process.setrlimit(Process::RLIMIT_STACK, CONF.THREAD_STACK_SIZE)
#      end

      @addr = nil

      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Processor", self)

      @deepconnect.when_disconnected do |deepspace, opts|
	when_disconnected(deepspace, opts)
      end

      for name, obj in EXPORTS
	export(name, obj)
      end

      require "fairy/share/inspector"
      @deepconnect.export("Inspector", Inspector.new(self))

      require "fairy/share/log"
      @node_deepspace = @deepconnect.open_deepspace("localhost", node_port)
      @node = @node_deepspace.import("Node")
      @logger = @node.logger
      Log.type = "[P]"
      Log.pid =id
      Log.logger = @logger
      Log::info self, "Processor Service Start"
      Log::info(self, "\tfairy version: #{Version}")
      Log::info(self, "\tRuby version: #{RUBY_VERSION}") 

      if CONF.PROCESSOR_MON_ON
	Log::info self, "Processor Status Monitoring: ON"
	start_process_status_monitor
      end

#      GC.disable
#      Thread.start do
#	loop do
#	  sleep 60
#	  GC.start
#	end
#      end
      @node.register_processor(self)
    end

    def terminate
      # clientが終了したときの終了処理
      Log::info(self, "Terminate!")
      Thread.start do
	begin
	# このメソッドが戻るまで待つ
	  sleep 0.2
	  @ntasks.each{|ntask| ntask.abort_running}
	  
	  @deepconnect.stop
	  Process.exit(0)
	rescue
	  Log::debug(self, "Exception Rised in termination ntasks.")
	  Log::debug_exception(self)
	end
      end
      nil
    end

    def terminate_all_ntasks
      Log::info(self, "Terminate all ntasks!!")
      begin
	@ntasks.each{|ntask| ntask.abort_running}
      rescue
	Log::debug(self, "Exception Rised in termination ntasks.")
	Log::debug_exception(self)
      end
    end

    def when_disconnected(deepspace, opts)
      Log::info self, "PROCESSOR: disconnected #{deepspace.peer_uuid}"
    end

    attr_accessor :addr
    attr_reader :node

    def set_stdout(peer)
      $stdout = Stdout.new(peer)
    end

    def node
      @node
    end

    def export(service, obj)
      @services[service] = obj
    end

    def import(service)
      svs = @services[service]
      unless svs
	ERR::Raise ERR::INTERNAL::NoRegisterService,  service
      end
      svs
    end

    def no_ntasks
      @ntasks.size
    end

    def create_ntask
      ntask = NTask.new(self)
      @ntasks.push ntask
      ntask
    end
    DeepConnect.def_method_spec(self, "REF create_ntask(VAL, *VAL)")

#     def create_njob(njob_class_name, bjob, opts, *rests)
#       klass = import(njob_class_name)
#       njob = klass.new(self, bjob, opts, *rests)
#       @njobs.push njob
#       Log.debug(self, "Njob number of %d", @njobs.size)
#       njob
#     end
#     DeepConnect.def_method_spec(self, "REF create_njob(VAL, REF, VAL, *VAL)")

    def create_import(policy)
      import = Import.new(policy)
      import.set_log_callback do |n| 
	Log::verbose(self, "IMPORT POP: #{n}")
      end
      import
    end
    DeepConnect.def_method_spec(self, "REF create_import(DVAL)")

    
    LIMIT_PROCESS_SIZE = 100  #kbyte
    def life_out_life_span?
#       puts "LOLS: #{inspectx}"
#       puts "njob: #{all_njob_finished?}"
#       unless all_njob_finished? 
# 	for njob, status in @njob_status
# 	  puts "#{njob.class} => #{status}"
# 	end
#       end

#       puts "varry: #{exist_varray_elements?}"

      return false unless all_ntasks_finished?
      return false if exist_varray_elements?

      # 取りあえず
      vsz = `ps -ovsz h#{Process.pid}`.to_i
#puts "vsz: #{vsz}, #{LIMIT_PROCESS_SIZE > vsz}"

      LIMIT_PROCESS_SIZE < vsz
    end

    #
    # varray management
    #
    def init_varray_feature
      @varray_elements = {}
      @varray_elements_mutex = Mutex.new
    end

    def exist_varray_elements?
      @varray_elements_mutex.synchronize do
	!@varray_elements.empty?
      end
    end

    def register_varray_element(array)
      @varray_elements_mutex.synchronize do
	@varray_elements[array.object_id] = array.object_id
      end
      ObjectSpace.define_finalizer(array, deregister_varray_element_proc)
    end

    def deregister_varray_element_proc
      proc do |oid|
	@varray_elements_mutex.synchronize do
	  @varray_elements.delete(oid)
	end
      end
    end

    #
    # ntask status management
    #
    def init_ntask_status_feature
      @ntask_status = {}
      @ntask_status_mutex = Mutex.new
      @ntask_status_cv = ConditionVariable.new
    end

    def all_ntask_finished?
      @ntask_status_mutex.synchronize do
	for node, status in @ntask_status
	  return false if status != :ST_FINISH
	end
      end
      true
    end

    def update_status(node, st)
      @ntask_status_mutex.synchronize do
	@ntask_status[node] = st
	@ntask_status_cv.broadcast
      end
    end

    #
    # prossessor monitoring
    #
    def start_process_status_monitor
      Thread.start do
	begin
	  idle = CONF.PROCESSOR_MON_INTERVAL
	  loop do
	    sleep idle 
	    process_status_mon
	  end
	rescue
	  Log::debug_exception
	  raise
	end
      end
    end

    def process_status_mon(inspect_p = CONF.PROCESSOR_MON_OBJECTSPACE_INSPECT_ON)

      if inspect_p
	GC.start

	count = 0
	count_by_class = {}
	ObjectSpace.each_object do |o|
	  count += 1
	  klass = o.__deep_connect_real_class
	  count_by_class[klass] = (count_by_class[klass] || 0) + 1
	end
	exp = 0
	exp_by_class = {}
	imp = 0
	for ds in @deepconnect.instance_eval{@organizer}.deep_spaces.values
	  exp_roots = ds.instance_eval{@export_roots}
	  exp += exp_roots.size
	  exp_roots.each do |k, v| 
	    klass = v.class
	    exp_by_class[klass] = (exp_by_class[klass] || 0) + 1
	  end
	  imp += ds.instance_eval{@import_reference.size}
	end
      end

      format = CONF.PROCESSOR_MON_PSFORMAT
      m = `ps -o#{format} h#{Process.pid}`.chomp
      Log::info(self) do |sio|
	sio.puts("PROCESS MONITOR:")
	sio.puts("#{Log.host} [P]\##{@id} MONITOR: PS: #{m}")
	if inspect_p
	  sio.puts("#{Log.host} [P]\##{@id} MONITOR: OBJECT: #{count}")
	  for klass in count_by_class.keys.sort_by{|k| k.name}
	    sio.puts("#{Log.host} [P]\##{@id} MONITOR: C: #{klass.name} => #{count_by_class[klass]}")
	  end
	  sio.puts("#{Log.host} [P]\##{@id} MONITOR: DEEP-CONNECT: exports: #{exp}")
	  for klass in exp_by_class.keys.sort_by{|k| k.name}
	    sio.puts("#{Log.host} [P]\##{@id} MONITOR: C: #{klass.name} => #{exp_by_class[klass]}")
	  end
	  sio.puts("#{Log.host} [P]\##{@id} MONITOR: DEEP-CONNECT: imports: #{imp}")
	end
      end
    end

    def inspectx
      "#<#{self.class}: #{id} [#{@ntask.collect{|n| n.class.name}.join(" ")}]>"
    end

  end

  def Processor.start(id, node_port)
    processor = Processor.new(id)
    processor.start(node_port)
  end

end

require "fairy/node/addins"
