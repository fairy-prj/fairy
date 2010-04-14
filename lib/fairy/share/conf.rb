# encoding: UTF-8

require "socket"
require "rbconfig"

module Fairy

  CONF_PATH = [
    "/etc/fairy.conf",
    RbConfig::CONFIG["libdir"]+"/fairy/etc/fairy.conf",
    ENV["FAIRY_HOME"] && ENV["FAIRY_HOME"]+"/etc/fairy.conf", 
    ENV["HOME"]+"/.fairyrc",
    ENV["FAIRY_CONF"]
  ]
#  $:.each{|p|
#    CONF_PATH.push p+"/fairy/etc/fairy.conf"
#  }

  class Conf
    PROPS = []
    class DefaultConf<Conf;end

    def initialize(super_conf = nil, opts = {})
      @super_conf = super_conf
      @values = opts
    end

    class<<Conf
      def def_prop(prop, reader = nil, setter = nil)
	PROPS.push prop
	reader = "def #{prop}; value(:#{prop}); end" unless reader
	setter = "def #{prop}=(value); @values[:#{prop}] = value; end" unless setter
	module_eval reader
	module_eval setter
#	DefaultConf.module_eval reader
      end

      def def_prop_relative_path(prop, path, base_prop = :HOME)
	def_prop(prop, 
		 "def #{prop}; value(:#{prop}) || self.#{base_prop}+'/'+'#{path}'; end")
      end
      alias def_rpath def_prop_relative_path
    end

    def value(prop)
      if (v = @values[prop]).nil?
	@super_conf && v = @super_conf.value(prop)
      end
      v
    end

    def props
      PROPS
    end

    def base_conf=(conf)
      if @super_conf
	@super_conf.base_conf = conf
      else
	@super_conf = conf
      end
    end

    def set_values(hash)
      for key, value in hash
	@values[key] = value
      end
    end

    def_prop :RUBY_BIN
    def_prop :MASTER_HOST
    def_prop :MASTER_PORT

    def_prop :HOME
    def HOME
      File.expand_path(value(:HOME))
    end

    def_rpath :BIN, "bin"
    def_rpath :CONTROLLER_BIN, "controller", :BIN
    def_rpath :PROCESSOR_BIN, "processor", :BIN
    def_rpath :LIB, "lib"

    def_prop :DEFAULT_EXTERNAL
    def_prop :DEFAULT_INTERNAL

    def_prop :IGNORE_EXCEPTION_ON_FILTER

    def_prop :MASTER_MAX_ACTIVE_PROCESSORS

    def_prop :CONTROLLER_INPUT_PROCESSOR_N
    def_prop :CONTROLLER_MAX_ACTIVE_NTASKS_IN_PROCESSOR

    def_prop :CONTROLLER_ASSIGN_NEW_PROCESSOR_N_FACTOR

    def_prop :PREQUEUING_POLICY
    def_prop :POSTQUEUING_POLICY

    def_prop :POSTMAPPING_POLICY

    def_prop :POSTQUEUE_MAX_TRANSFER_SIZE
    def_prop :POOLQUEUE_POOL_THRESHOLD
    def_prop :ONMEMORY_SIZEDQUEUE_SIZE
    def_prop :FILEBUFFEREDQUEUE_THRESHOLD

    def_prop :SORTEDQUEUE_POOL_THRESHOLD
    def_prop :SORTEDQUEUE_THRESHOLD
    def_prop :SORTEDQUEUE_SORTBY

    def_prop :LOCAL_INPUT_FILE_BUFFER_SIZE
    def_prop :HERE_POOL_THRESHOLD
    def_prop :LOCAL_OUTPUT_POOL_THRESHOLD

    def_prop :N_MOD_GROUP_BY
    def_prop :HASH_MODULE
    def_prop :HASH_OPTIMIZE

    def_prop :MOD_GROUP_BY_BUFFERING_POLICY
    def_prop :MOD_GROUP_BY_CMSB_THRESHOLD
    def_prop :MOD_GROUP_BY_CMSB_CHUNK_SIZE

    def_prop :BARRIER_MEMORY_BUFFERING_POLICY

    def_prop :SORT_SAMPLING_MIN
    def_prop :SORT_SAMPLING_MAX
    def_prop :SORT_SAMPLING_RATIO_1_TO
    def_prop :SORT_N_GROUP_BY

    def_prop :IOTA_SPLIT_NO

    def_prop :TRANSFAR_MARSHAL_STRING_ARRAY_OPTIMIZE

    def_rpath :VF_ROOT, "Repos"
    def_prop :VF_PREFIX
    def_prop :VF_BASE_NAME_CONVERTER
    def_prop :VF_SPLIT_SIZE

    def_prop :TMP_DIR

    def_prop :LOG_FILE
    def_prop :LOG_LEVEL
    def_prop :LOG_FLUSH_INTERVAL
    def_prop :LOG_IMPORT_NTIMES_POP
    def_prop :LOG_LOCAL_OUTPUT_DEV 

    def_prop :PROCESSOR_MON_ON
    def_prop :PROCESSOR_MON_INTERVAL
    def_prop :PROCESSOR_MON_PSFORMAT
    def_prop :PROCESSOR_MON_OBJECTSPACE_INSPECT_ON

    def_prop :SOCK_DO_NOT_REVERSE_LOOKUP
    def_prop :USE_RESOLV_REPLACE
    
    def_prop :BLOCK_USE_STDOUT

    def_prop :DEBUG_PORT_WAIT
    def_prop :DEBUG_FULL_BACKTRACE
    def_prop :DEBUG_THREAD_ABORT_ON_EXCEPTION
    def_prop :DEBUG_MONITOR_ON
    def_prop :DEBUG_PROCESSOR_TRACE_ON
    def_prop :DEBUG_BUG49 

    def_prop :PROCESS_LIFE_MANAGE_INTERVAL

    def_prop :THREAD_STACK_SIZE

#     class JobConf<Conf
#       def initialize(prefix = "", super_conf = nil, opts ={})
# 	@prefix = prefix.upcase
#       end

#       def method_missing(method, *args, &b)
# 	del = "#{prefix}_#{method.id2name.upcase}"
# 	if respond_to?(del)
# 	  __send__(del, *args, &b) 
# 	else
# 	  super
# 	end
#       end
#     end
#     BJobConf = JobConf
#     NJobConf = JobConf

    class DefaultConf

      def initialize
	super

	@default_host = `hostname`.chomp
	@hosts = {}
      end

      def [](host)
	unless @hosts[host]
	  @hosts[host] = Conf.new
	end
	@hosts[host]
      end

      def value(prop)
	conf = @hosts[@default_host]
	v = conf && conf.value(prop)
	return v if v
	@values[prop]
      end

      def load_all_conf
	loaded = false
	for path in CONF_PATH
	  if path
	    if File.exist?(path)
	      load_conf path
	      loaded = true
	    end
	  end
	end
#	unless loaded
#	  puts "warnfairy.confファイルが見つかりません"
#	end
      end

      def load_conf(conf)
	begin
	  load conf
	rescue LoadError
	  puts "設定ファイル #{conf} をロードできませんでした."
	  exit 1
	rescue
	  puts "設定ファイル #{@conf} をロード中に例外が発生しました."
	  p $!
	  p $@
	  exit 2
	end
      end
    end
  end

  DEFAULT_CONF = Conf::DefaultConf.new
  
  DEFAULT_CONF.RUBY_BIN = ENV["FAIRY_RUBY"] || "ruby" 

  #DEFAULT_CONF.MASTER_HOST = 
  DEFAULT_CONF.MASTER_PORT = 19999

  DEFAULT_CONF.HOME = ENV["FAIRY_HOME"] || "/usr/lib/fairy"
#  DEFAULT_CONF.BIN = DEFAULT_CONF.HOME+"/bin"
#  DEFAULT_CONF.LIB = DEFAULT_CONF.HOME+"/lib"

#  DEFAULT_CONF.CONTROLLER_BIN = DEFAULT_CONF.BIN+"/controller"
#  DEFAULT_CONF.PROCESSOR_BIN = DEFAULT_CONF.BIN+"/processor"

  DEFAULT_CONF.DEFAULT_EXTERNAL = nil
  DEFAULT_CONF.DEFAULT_INTERNAL = nil

  DEFAULT_CONF.IGNORE_EXCEPTION_ON_FILTER = false

  DEFAULT_CONF.MASTER_MAX_ACTIVE_PROCESSORS = 4

  DEFAULT_CONF.CONTROLLER_INPUT_PROCESSOR_N = 10
  DEFAULT_CONF.CONTROLLER_MAX_ACTIVE_NTASKS_IN_PROCESSOR = 4

  DEFAULT_CONF.CONTROLLER_ASSIGN_NEW_PROCESSOR_N_FACTOR = 1

  DEFAULT_CONF.POSTMAPPING_POLICY = nil

  DEFAULT_CONF.PREQUEUING_POLICY = {:queuing_class => :ChunkedSizedPoolQueue}
  DEFAULT_CONF.POSTQUEUING_POLICY = {:queuing_class => :ChunkedFileBufferdQueue}

  DEFAULT_CONF.POSTQUEUE_MAX_TRANSFER_SIZE = 100000
  DEFAULT_CONF.POOLQUEUE_POOL_THRESHOLD = 10000
  DEFAULT_CONF.ONMEMORY_SIZEDQUEUE_SIZE = 10000
  DEFAULT_CONF.FILEBUFFEREDQUEUE_THRESHOLD = 10000/2
  
  DEFAULT_CONF.SORTEDQUEUE_POOL_THRESHOLD = DEFAULT_CONF.POOLQUEUE_POOL_THRESHOLD
  DEFAULT_CONF.SORTEDQUEUE_THRESHOLD = 10000/2
  DEFAULT_CONF.SORTEDQUEUE_SORTBY = %{|v| v}

  DEFAULT_CONF.LOCAL_INPUT_FILE_BUFFER_SIZE = 1024*1024
  DEFAULT_CONF.HERE_POOL_THRESHOLD = 32000
  DEFAULT_CONF.LOCAL_OUTPUT_POOL_THRESHOLD = 32000

  DEFAULT_CONF.N_MOD_GROUP_BY = 5
  DEFAULT_CONF.HASH_MODULE = "fairy/share/hash-md5"
  DEFAULT_CONF.HASH_OPTIMIZE = false

  DEFAULT_CONF.MOD_GROUP_BY_BUFFERING_POLICY = {:buffering_class => :MergeSortBuffer}
  DEFAULT_CONF.MOD_GROUP_BY_CMSB_THRESHOLD = 100_000
  DEFAULT_CONF.MOD_GROUP_BY_CMSB_CHUNK_SIZE = 10000

  DEFAULT_CONF.BARRIER_MEMORY_BUFFERING_POLICY = {:queuing_class => :PoolQueue}

  DEFAULT_CONF.SORT_SAMPLING_MIN = 100
  DEFAULT_CONF.SORT_SAMPLING_MAX = 10000
  DEFAULT_CONF.SORT_SAMPLING_RATIO_1_TO = 100
  DEFAULT_CONF.SORT_N_GROUP_BY = DEFAULT_CONF.N_MOD_GROUP_BY

  DEFAULT_CONF.IOTA_SPLIT_NO = 4

  DEFAULT_CONF.TRANSFAR_MARSHAL_STRING_ARRAY_OPTIMIZE = true

#  DEFAULT_CONF.VF_ROOT = DEFAULT_CONF.HOME+"/Repos"
  DEFAULT_CONF.VF_PREFIX = `hostname`.chomp
## DEFAULT_CONF.VF_PREFIX is client setting.
  DEFAULT_CONF.VF_BASE_NAME_CONVERTER = nil
  DEFAULT_CONF.VF_SPLIT_SIZE = 64*1024*1024

  DEFAULT_CONF.TMP_DIR = "/tmp/fairy/tmpbuf"

  DEFAULT_CONF.LOG_FILE = "/tmp/fairy/log"
  DEFAULT_CONF.LOG_FLUSH_INTERVAL = 1
  DEFAULT_CONF.LOG_LEVEL = :DEBUG
  DEFAULT_CONF.LOG_IMPORT_NTIMES_POP = 100000
  DEFAULT_CONF.LOG_LOCAL_OUTPUT_DEV = :$stderr

  DEFAULT_CONF.PROCESSOR_MON_ON = false
  DEFAULT_CONF.PROCESSOR_MON_INTERVAL = 60
  DEFAULT_CONF.PROCESSOR_MON_PSFORMAT = "stat,vsz,rss,sz,pmem,pcpu,nlwp,time,wchan"
  DEFAULT_CONF.PROCESSOR_MON_OBJECTSPACE_INSPECT_ON = false

  DEFAULT_CONF.SOCK_DO_NOT_REVERSE_LOOKUP = true
  DEFAULT_CONF.USE_RESOLV_REPLACE = false

  DEFAULT_CONF.BLOCK_USE_STDOUT = true

  DEFAULT_CONF.DEBUG_PORT_WAIT = false
  DEFAULT_CONF.DEBUG_FULL_BACKTRACE = false
  DEFAULT_CONF.DEBUG_THREAD_ABORT_ON_EXCEPTION = false
  DEFAULT_CONF.DEBUG_MONITOR_ON = false
  DEFAULT_CONF.DEBUG_PROCESSOR_TRACE_ON = false
  DEFAULT_CONF.DEBUG_BUG49 = false

# DEFAULT_CONF.PROCESS_LIFE_MANAGE_INTERVAL = 60
# DEFAULT_CONF.PROCESS_LIFE_MANAGE_INTERVAL = 10
# DEFAULT_CONF.PROCESS_LIFE_MANAGE_INTERVAL = 1
  DEFAULT_CONF.PROCESS_LIFE_MANAGE_INTERVAL = nil

#  DEFAULT_CONF.THREAD_STACK_SIZE = 1024*100

  CONF = DEFAULT_CONF
  CONF.load_all_conf

  def REPLACE_CONF(conf)
    remove_const(:CONF)
    const_set(:CONF, conf)
  end
  module_function :REPLACE_CONF

  
  def Conf.configure_common_conf
    Thread.abort_on_exception = CONF.DEBUG_THREAD_ABORT_ON_EXCEPTION

    TCPSocket.do_not_reverse_lookup = CONF.SOCK_DO_NOT_REVERSE_LOOKUP

    if CONF.USE_RESOLV_REPLACE
      require "resolv-replace"
    end
  end
end
