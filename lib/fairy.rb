# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "thread"
require "xthread"
require "fiber-mon"

require "deep-connect"

require "fairy/version"
require "fairy/share/conf"
require "fairy/share/log"
require "fairy/share/locale"
require "fairy/share/encoding"

module Fairy
  Conf.configure_common_conf

  @USER_LEVEL_FILTERS = {}

  def Fairy::def_filter(name, opts={}, &definition)
    name = name.intern if name.kind_of?(String)
    @USER_LEVEL_FILTERS[name] = definition

    interface_mod = Module.new

    if !opts[:sub]
      interface_mod.module_eval %{
        def #{name}(*args)
	  p = ::Fairy::user_level_filter(:#{name})
	  ::Fairy::ERR.Raise ::Fairy::ERR::INTERNAL::NoSuchDefiledUserLevelFilter, name unless p
	  p.call(@fairy, self, *args)			     
        end
      }
    else
      interface_mod.module_eval %{
        def #{name}(*args)
	  p = ::Fairy::user_level_filter(:#{name})
	  ::Fairy::ERR.Raise ::Fairy::ERR::INTERNAL::NoSuchDefiledUserLevelFilter, name unless p
  	  sub{|subf, input| p.call(subf, input, *args)}			     
        end
      }
    end
  Fairy.def_filter_interface interface_mod
  end

  def Fairy::user_level_filter(name)
    @USER_LEVEL_FILTERS[name]
  end

  class Fairy

    def self.create_subfairy(fairy)
      subfairy = Fairy.allocate
      subfairy.initialize_subfairy(fairy)
      subfairy
    end

    def initialize(master_host = CONF.MASTER_HOST, 
		   master_port = CONF.MASTER_PORT,
		   opts = {})

      if master_host.kind_of?(Hash)
	opts = master_host
	master_host = CONF.MASTER_HOST
	master_port = CONF.MASTER_PORT
      end

      ::Fairy::REPLACE_CONF(Conf.new(CONF, opts))

      Thread.abort_on_exception = CONF.DEBUG_THREAD_ABORT_ON_EXCEPTION

      @name2backend_class = {}

      @deep_connect = DeepConnect.start(0)
      @master_deepspace = @deep_connect.open_deepspace(master_host, master_port)
      @master = @master_deepspace.import("Master")

      @controller = @master.assgin_controller
      @controller.connect(self, CONF)

      @logger = @master.logger
      Log.type = "[c]"
      Log.pid = @controller.id
      Log.logger = @logger
      Log.set_local_output_dev
      Log::info self, "fairy connected!!"
      Log::info self, "\tfairy version: #{Version}"
      Log::info(self, "\t[Powered By #{RUBY_DESCRIPTION}]") 

      if EXT_FAIRY
	Log::warn self, "\t Load fairy.so"
      else
	Log::warn self, "Can't load fairy.so. Can't use this feature"
      end

      @stdout_mutex = Mutex.new

      if CONF.DEBUG_MONITOR_ON
	Log::info self, "MONITOR NODE: ON"
	require "fairy/share/debug"
	Debug::njob_status_monitor_on(self)
      end
    end

    def initialize_subfairy(fairy)
      @name2backend_class = {}
      @deep_connect = fairy.instance_eval{@deep_connect}
      @master_deepspace = fairy.instance_eval{@master_deepspace}
      @master = fairy.instance_eval{@master}

      @controller = @master.assgin_controller
      @controller.connect(self, CONF)
      
      # Logは親と共有される
      # なので, IDは親と同じになる(process idなので当たり前)
      
      @stdout_mutex = fairy.instance_eval{@stdout_mutex}
    end

    attr_reader :controller

    def abort
      @master.terminate_controller(@controller)
    end

    def name2backend_class(backend_class_name)
      if klass = @name2backend_class[backend_class_name]
	return klass 
      end
      
      if klass =  @controller.import(backend_class_name)
	@name2backend_class[backend_class_name] = klass
      end
      klass
    end

    # pool variables
    def def_pool_variable(vname, value = nil)
      @controller.def_pool_variable(vname, value)
    end

    def pool_variable(vname, *value)
      @controller.pool_variable(vname, *value)
    end

    # exception handling
    def handle_exception(exp)
      local_exp = nil
      Log::debug(self, "exception raised: #{exp.class}")
      Log::debug_exception(self, exp)
      begin
	local_exp = exp.dc_deep_copy
      rescue Exception
	Thread.main.raise exp
	raise exp
      end
      Thread.main.raise local_exp
      nil
    end

    # debug print
    def stdout_write(str)
      @stdout_mutex.synchronize do
	$stdout.write(str)
      end
    end

    # external module loading
    def self.def_fairy_interface(mod)
      include mod
    end
  end

  class FilterChain
    def initialize(input)
      @filters = [input]
    end

    def [](idx)
      @filters[idx]
    end

    def show_chain
      @filters.each_with_index{|f, idx|
        puts "[#{idx}]\t#{f.class}" 
      }
    end

    def method_missing(msg, *args, &block)
      #pp msg
      ret = @filters.last.__send__(msg, *args, &block)
      if ret.kind_of?(Job)
        @filters << ret
        self
      else
        ret
      end
    end
  end

  def def_fairy_interface(mod)
    ::Fairy::Fairy.instance_eval{include mod}
  end
  module_function :def_fairy_interface
end

require "fairy/client/filter"
require "fairy/client/input"

require "fairy/client/addins"

begin
  require "fairy.so"
  EXT_FAIRY = true
rescue LoadError
  EXT_FAIRY = false
end

