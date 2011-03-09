# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fiber-mon"
require "deep-connect"

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
      @ntask_seq = -1
      @ntask_seq_mutex = Mutex.new

      @njob_mon = FiberMon.new

      init_varray_feature
      init_ntask_status_feature
    end

    attr_reader :id
    attr_reader :ntasks

    attr_reader :njob_mon

    def log_id
      "Processor[#{@id}]"
    end

    attr_reader :deepconnect

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

      #@njob_mon.start

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
      Log::info(self, "\t[Powered By #{RUBY_DESCRIPTION}]") 

      start_watch_status


#      GC.disable
#      Thread.start do
#	loop do
#	  sleep 60
#	  GC.start
#	end
#      end
      @node.register_processor(self)
    end

    def connect_controller(controller, conf)
      @controller = controller
      conf.base_conf = CONF
      Fairy::REPLACE_CONF(conf)

#      Log::set_local_output_dev

      if CONF.PROCESSOR_MON_ON
	Log::info self, "Processor Status Monitoring: ON"
	start_process_status_monitor
      end

      $stdout = Stdout.new(controller)
    end
    DeepConnect.def_method_spec(self, "REF connect_controller(REF, DVAL)")

    def terminate
      # clientが終了したときの終了処理
      Log::info(self, "terminate!!")
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
      Log::debug(self, "Terminate all ntasks!!")
      begin
	@ntasks.each{|ntask| ntask.abort_running}
      rescue
	Log::debug(self, "Exception Rised in termination ntasks.")
	Log::debug_exception(self)
      end
    end

    def when_disconnected(deepspace, opts)
      Log::debug self, "PROCESSOR: disconnected #{deepspace.peer_uuid}"
    end

    attr_accessor :addr
    attr_reader :node

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

    def ntask_next_id
      @ntask_seq_mutex.synchronize do
	@ntask_seq += 1
      end
    end

    def no_ntasks
      @ntasks.size
    end

    def create_ntask
      ntask = PTask.new(ntask_next_id, self)
      @ntasks.push ntask
      ntask
    end
    DeepConnect.def_method_spec(self, "REF create_ntask(VAL, *VAL)")

#     def create_njob(njob_class_name, bjob, opts, *rests)
#       klass = import(njob_class_name)
#       njob = klass.new(self, bjob, opts, *rests)
#       @njobs.push njob
#       Log.debugf(self, "Njob number of %d", @njobs.size)
#       njob
#     end
#     DeepConnect.def_method_spec(self, "REF create_njob(VAL, REF, VAL, *VAL)")

    def create_import(policy)
      import = Import.new(policy)
      import.set_log_callback do |n, key| 
	Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
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
    # (processor) status management and ntask status management
    # processor status:
    #   ST_WAIT
    #	ST_ACTIVATE
    #
    def init_ntask_status_feature
      @status = :ST_WAIT
      @ntask_status = {}

      @status_mx = @njob_mon.new_mon
      @status_cv = @status_mx.new_cv

    end

    ACTIVE_STATUS = {
      :ST_INIT => true,
      :ST_WAIT_IMPORT => true, 
      :ST_ACTIVATE => true
    }

    SEMIACTIVE_STATUS = {
#      :ST_INIT => true,
#      :ST_WAIT_IMPORT => true, 
      :ST_ALL_IMPORTED => true, 
      :ST_WAIT_EXPORT_FINISH => true, 
      :ST_EXPORT_FINISH => true, 
      :ST_OUTPUT_FINISH => true
    }

    def no_active_ntasks
      no_actives = 0
      @ntask_status.each{|ntask, st|
	no_actives += 1 if ACTIVE_STATUS[st]
      }
      no_actives
    end

    def all_ntasks_finished?(lock = :lock)
      if lock == :lock
	@status_cv.synchronize do
	  all_ntasks_finished_no_lock?
	end
      else
	all_ntasks_finished_no_lock?
      end
    end

    def all_ntasks_finished_no_lock?
      for node, status in @ntask_status
	return false if status != :ST_FINISH
      end
      true
    end

    def all_ntasks_semiactivated?(lock = :lock)
      if lock == :lock
	@status_cv.synchronize do
	  all_ntasks_semiactivated_no_lock?
	end
      else
	all_ntasks_semiactivated_no_lock?
      end
    end

    def all_ntasks_semiactivated_no_lock?
      for node, status in @ntask_status
	return false unless SEMIACTIVE_STATUS[status]
      end
      true
    end


    def update_status(ntask, st)
Log::debug(self, "UPDATE_STATUS: #{ntask}, #{st}")
Log::debug(self, "A3:1");
      @status_mx.synchronize do
Log::debug(self, "A3:2");
	@ntask_status[ntask] = st

	case st
	when :ST_INIT
	  # do nothing
	  if all_ntasks_semiactivated?(:no_lock)
Log::debug(self, "UPDATE_STATUS A: #{st}")
	    @status = :ST_SEMIACTIVATE
	  end
	when :ST_WAIT_IMPORT
	  if all_ntasks_semiactivated?(:no_lock)
Log::debug(self, "UPDATE_STATUS B: #{st}")
	    @status = :ST_SEMIACTIVATE
	  end
	when :ST_ACTIVATE
Log::debug(self, "UPDATE_STATUS C: #{st}")
	  @status = :ST_ACTIVATE
	when :ST_ALL_IMPORTED, 
	    :ST_WAIT_EXPORT_FINISH, 
	    :ST_EXPORT_FINISH, 
	    :ST_OUTPUT_FINISH
	  if all_ntasks_semiactivated?(:no_lock)
Log::debug(self, "UPDATE_STATUS D: #{st}")
	    @status = :ST_SEMIACTIVATE
	  end
	when :ST_FINISH
	  if all_ntasks_finished?(:no_lock)
Log::debug(self, "UPDATE_STATUS E: #{st}")
	    @status = :ST_WAIT
	  end
	else
	  if @status == :ST_WAIT
Log::debug(self, "UPDATE_STATUS F: #{st}")
	    @status = :ST_ACTIVATE
	  end
Log::debug(self, "A3:3");
	end
Log::debug(self, "A3:4");
	@status_cv.broadcast
      end
Log::debug(self, "A3:5");
    end

    def start_watch_status
      # 初期状態通知
Log::debug(self, "B1:1");
      notice_status(@status)
Log::debug(self, "B1:2");

      @njob_mon.entry do
Log::debug(self, "B1:3");
	@status_mx.synchronize do
Log::debug(self, "B1:4");
	  old_status = nil
	  old_no_active_ntasks = 0
Log::debug(self, "B1:5");
	  loop do
Log::debug(self, "B1:6");
	    @status_cv.wait_while{
	      old_status == @status && old_no_active_ntasks == no_active_ntasks
	    }
Log::debug(self, "B1:7");
	    no = no_active_ntasks
	    if old_no_active_ntasks != no
Log::debug(self, "B1:8");
	      old_no_active_ntasks = no
Log::debug(self, "B1:9");
	      @controller.update_active_ntasks(self, no)
Log::debug(self, "B1:A");
	    end
	    if old_status != @status
Log::debug(self, "B1:B");
	      old_status = @status
Log::debug(self, "B1:C");
	      notice_status(@status)
Log::debug(self, "B1:D");
	    end
Log::debug(self, "B1:E");
	  end
Log::debug(self, "B1:F");
	end
Log::debug(self, "B1:G");
      end
Log::debug(self, "B1:H");
      nil
    end

    def notice_status(st)
      @node.update_processor_status(self, st)
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

    def to_s
      "#<#{self.class}: #{id}>"
    end

  end

  def Processor.start(id, node_port)
    processor = Processor.new(id)
    processor.start(node_port)
  end

end

require "fairy/node/addins"
