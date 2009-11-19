# encoding: UTF-8

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

    def initialize
      @values = {}
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
      @values[prop]
    end

    def props
      PROPS
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

    def_prop :MASTER_MAX_ACTIVE_PROCESSORS

    def_prop :CONTROLLER_INPUT_PROCESSOR_N

    def_prop :CONTROLLER_ASSIGN_NEW_PROCESSOR_N_FACTOR

    def_prop :PREQUEUING_POLICY
    def_prop :POSTQUEUING_POLICY

    def_prop :POSTMAPPING_POLICY

    def_prop :POSTQUEUE_MAX_TRANSFER_SIZE
    def_prop :POOLQUEUE_POOL_THRESHOLD
    def_prop :ONMEMORY_SIZEDQUEUE_SIZE
    def_prop :FILEBUFFEREDQUEUE_THRESHOLD

    def_prop :LOCAL_INPUT_FILE_BUFFER_SIZE
    def_prop :HERE_POOL_THRESHOLD
    def_prop :LOCAL_OUTPUT_POOL_THRESHOLD

    def_prop :N_MOD_GROUP_BY
    def_prop :HASH_MODULE
    def_prop :HASH_OPTIMIZE

    def_prop :MOD_GROUP_BY_BUFFERING_POLICY
    def_prop :MOD_GROUP_BY_CMSB_THRESHOLD

    def_prop :BARRIER_MEMORY_BUFFERING_POLICY

    def_prop :SORT_SAMPLING_MIN
    def_prop :SORT_SAMPLING_MAX
    def_prop :SORT_SAMPLING_RATIO_1_TO
    def_prop :SORT_N_GROUP_BY

    def_rpath :VF_ROOT, "Repos"
    def_prop :VF_PREFIX
    def_prop :VF_SPLIT_SIZE

    def_prop :TMP_DIR

    def_prop :LOG_FILE
    def_prop :LOG_LEVEL
    def_prop :LOG_FLUSH_INTERVAL
    def_prop :LOG_IMPORT_NTIMES_POP

    def_prop :PROCESSOR_MON_ON
    def_prop :PROCESSOR_MON_INTERVAL
    def_prop :PROCESSOR_MON_PSFORMAT
    def_prop :PROCESSOR_MON_OBJECTSPACE_INSPECT_ON

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

  CONF = Conf::DefaultConf.new
  
  CONF.RUBY_BIN = ENV["FAIRY_RUBY"] || "ruby" 

  #CONF.MASTER_HOST = 
  CONF.MASTER_PORT = 19999

  CONF.HOME = ENV["FAIRY_HOME"] || "/usr/lib/fairy"
#  CONF.BIN = CONF.HOME+"/bin"
#  CONF.LIB = CONF.HOME+"/lib"

#  CONF.CONTROLLER_BIN = CONF.BIN+"/controller"
#  CONF.PROCESSOR_BIN = CONF.BIN+"/processor"

  CONF.DEFAULT_EXTERNAL = nil
  CONF.DEFAULT_INTERNAL = nil

  CONF.MASTER_MAX_ACTIVE_PROCESSORS = 4

  CONF.CONTROLLER_INPUT_PROCESSOR_N = 10

  CONF.CONTROLLER_ASSIGN_NEW_PROCESSOR_N_FACTOR = 1

  CONF.POSTMAPPING_POLICY = nil

  CONF.PREQUEUING_POLICY = {:queuing_class => :OnMemoryQueue}
  CONF.POSTQUEUING_POLICY = {:queuing_class => :OnMemoryQueue}

  CONF.POSTQUEUE_MAX_TRANSFER_SIZE = 10000
  CONF.POOLQUEUE_POOL_THRESHOLD = 1000
  CONF.ONMEMORY_SIZEDQUEUE_SIZE = 10000
  CONF.FILEBUFFEREDQUEUE_THRESHOLD = 10000/2

  CONF.LOCAL_INPUT_FILE_BUFFER_SIZE = 1024*1024
  CONF.HERE_POOL_THRESHOLD = 32000
  CONF.LOCAL_OUTPUT_POOL_THRESHOLD = 32000

  CONF.N_MOD_GROUP_BY = 5
  CONF.HASH_MODULE = "fairy/share/hash-md5"
  CONF.HASH_OPTIMIZE = false

  CONF.MOD_GROUP_BY_BUFFERING_POLICY = {:buffering_class => :OnMemoryBuffer}
  CONF.MOD_GROUP_BY_CMSB_THRESHOLD = 10000

  CONF.BARRIER_MEMORY_BUFFERING_POLICY = {:queuing_class => :OnMemoryQueue}

  CONF.SORT_SAMPLING_MIN = 100
  CONF.SORT_SAMPLING_MAX = 10000
  CONF.SORT_SAMPLING_RATIO_1_TO = 100
  CONF.SORT_N_GROUP_BY = CONF.N_MOD_GROUP_BY

#  CONF.VF_ROOT = CONF.HOME+"/Repos"
  CONF.VF_PREFIX = `hostname`.chomp
## CONF.VF_PREFIX is client setting.
  CONF.VF_SPLIT_SIZE = 64*1024*1024

  CONF.TMP_DIR = "/tmp/fairy/tmpbuf"

  CONF.LOG_FILE = "/tmp/fairy/log"
  CONF.LOG_FLUSH_INTERVAL = 1
  CONF.LOG_LEVEL = :DEBUG
  CONF.LOG_IMPORT_NTIMES_POP = 10000

  CONF.PROCESSOR_MON_ON = false
  CONF.PROCESSOR_MON_INTERVAL = 60
  CONF.PROCESSOR_MON_PSFORMAT = "stat,vsz,rss,sz,pmem,pcpu,nlwp,time,wchan"
  CONF.PROCESSOR_MON_OBJECTSPACE_INSPECT_ON = false

  CONF.USE_RESOLV_REPLACE = false

  CONF.BLOCK_USE_STDOUT = true

  CONF.DEBUG_PORT_WAIT = false
  CONF.DEBUG_FULL_BACKTRACE = false
  CONF.DEBUG_THREAD_ABORT_ON_EXCEPTION = false
  CONF.DEBUG_MONITOR_ON = false
  CONF.DEBUG_PROCESSOR_TRACE_ON = false
  CONF.DEBUG_BUG49 = false

# CONF.PROCESS_LIFE_MANAGE_INTERVAL = 60
# CONF.PROCESS_LIFE_MANAGE_INTERVAL = 10
# CONF.PROCESS_LIFE_MANAGE_INTERVAL = 1
  CONF.PROCESS_LIFE_MANAGE_INTERVAL = nil

#  CONF.THREAD_STACK_SIZE = 1024*100

  CONF.load_all_conf

end
