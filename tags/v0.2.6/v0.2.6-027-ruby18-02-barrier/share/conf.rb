
module Fairy

  CONF_PATH = [
    "/etc/fairy.conf",
    ENV["HOME"]+"/.fairyrc",
    ENV["FAIRY_CONF"],
    "etc/fairy.conf"
  ]
    

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

    def_prop :LOG_FILE
    def_prop :LOG_LEVEL
    def_prop :LOG_FLUSH_INTERVAL

    def_prop :VF_ROOT
    def_prop :VF_PREFIX

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
	for path in CONF_PATH
	  if path
	    if File.exist?(path)
	      load path
	    end
	  end
	end
      end
    end
  end

  CONF = Conf::DefaultConf.new
  CONF.load_conf

end
