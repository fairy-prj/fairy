
require "deep-connect/deep-connect"

require "share/conf"
require "share/stdout"
require "share/log"

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
	  raise "クラス以外を登録するときにはサービス名が必要です(%{obj})"
	end
      end

      EXPORTS.push [name, obj]
    end

    def initialize(id)
      @id = id
      @reserve = 0

      @services = {}

      @njobs = []

      init_varray_feature
      init_njob_status_feature
    end

    attr_reader :id
    attr_reader :njobs

    def start(node_port, service=0)
      @addr = nil

      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Processor", self)

      for name, obj in EXPORTS
	export(name, obj)
      end

      @node_deepspace = @deepconnect.open_deepspace("localhost", node_port)
      @node = @node_deepspace.import("Node")
      @logger = @node.logger
      Log.type = "PROC"
      Log.pid =id
      Log.logger = @logger
      Log::info self, "Processor Service Start"

      @node.register_processor(self)
    end

    def terminate
      # clientが終了したときの終了処理
      Thread.start do
	# このメソッドが戻るまで待つ
	sleep 0.1
	@deepconnect.stop
	Process.exit(0)
      end
      nil
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
	raise "サービス(#{service})が登録されていません"
      end
      svs
    end

    def no_njobs
      @njobs.size
    end

    def create_njob(njob_class_name, bjob, opts, *rests)
      klass = import(njob_class_name)
      njob = klass.new(self, bjob, opts, *rests)
      @njobs.push njob
      njob
    end
    DeepConnect.def_method_spec(self, "REF create_njob(VAL, REF, VAL, *VAL)")

    
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

      return false unless all_njob_finished?
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
    # njob status management
    #
    def init_njob_status_feature
      @njob_status = {}
      @njob_status_mutex = Mutex.new
      @njob_status_cv = ConditionVariable.new
    end

    def all_njob_finished?
      @njob_status_mutex.synchronize do
	for node, status in @njob_status
	  return false if status != :ST_FINISH
	end
      end
      true
    end

    def update_status(node, st)
      @njob_status_mutex.synchronize do
	@njob_status[node] = st
	@njob_status_cv.broadcast
      end
    end

    def inspectx
      "#<#{self.class}: #{id} [#{@njobs.collect{|n| n.class.name}.join(" ")}]>"
    end

  end

  def Processor.start(id, node_port)
    processor = Processor.new(id)
    processor.start(node_port)
  end

end

require "node/addins"
