# encoding: UTF-8

module Fairy

  CONF_PATH = [
    "/etc/fairy.conf",
    ENV["HOME"]+"/.fairyrc",
    ENV["FAIRY_HOME"] && ENV["FAIRY_HOME"]+"/etc/fairy.conf", 
    ENV["FAIRY_CONF"],
    "etc/fairy.conf" # あまりよろしくない...
  ]
  $:.each{|p|
    CONF_PATH.push p+"/fairy/etc/fairy.conf"
  }

  class Conf
    class DefaultConf<Conf;end

    def initialize
      @values = {}
    end

    class<<Conf
      def def_prop(prop)
	module_eval "def #{prop}(value); @values[:#{prop}]; end"
	module_eval "def #{prop}=(value); @values[:#{prop}] = value; end"
	DefaultConf.module_eval "def #{prop}; value(:#{prop}); end"
      end
    end

    def value(prop)
      @values[prop]
    end

    def_prop :RUBY_BIN
    def_prop :MASTER_HOST
    def_prop :MASTER_PORT
    def_prop :HOME
    def_prop :BIN
    def_prop :CONTROLLER_BIN
    def_prop :PROCESSOR_BIN
    def_prop :LIB

    def_prop :PREQUEUING_POLICY
    def_prop :POSTQUEUING_POLICY
    def_prop :ONMEMORY_SIZEDQUEUE_SIZE
    def_prop :FILEBUFFEREDQUEUE_THRESHOLD

    def_prop :N_MOD_GROUP_BY
    def_prop :HASH_MODULE
    def_prop :MOD_GROUP_BY_BUFFERING_POLICY
    def_prop :MOD_GROUP_BY_CMSB_THRESHOLD

    def_prop :SORT_SAMPLING_MIN
    def_prop :SORT_SAMPLING_MAX
    def_prop :SORT_SAMPLING_RATIO_1_TO
    def_prop :SORT_N_GROUP_BY

    def_prop :VF_ROOT
    def_prop :VF_PREFIX
    def_prop :VF_SPLIT_SIZE

    def_prop :TMP_DIR

    def_prop :LOG_FILE
    def_prop :LOG_LEVEL
    def_prop :LOG_FLUSH_INTERVAL
    def_prop :LOG_IMPORT_NTIMES_POP

    def_prop :USE_RESOLV_REPLACE
    
    def_prop :DEBUG_PORT_WAIT
    def_prop :DEBUG_FULL_BACKTRACE
    def_prop :DEBUG_THREAD_ABORT_ON_EXCEPTION
    def_prop :DEBUG_MONITOR_ON

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

      def load_conf
	loaded = false
	for path in CONF_PATH
	  if path
	    if File.exist?(path)
	      load path
	      loaded = true
	    end
	  end
	end

	unless loaded
	  raise "fairy.confファイルが見つかりません"
	end

      end
    end
  end

  CONF = Conf::DefaultConf.new
  CONF.load_conf

end