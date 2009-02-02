# encoding: UTF-8

require "thread"
require "deep-connect/deep-connect.rb"

require "share/conf"
require "share/log"

#DeepConnect::Organizer.immutable_classes.push Array


module Fairy

  @USER_LEVEL_FILTERS = {}

  def Fairy::def_filter(name, opts={}, &definition)
    name = name.intern if name.kind_of?(String)
    @USER_LEVEL_FILTERS[name] = definition

    interface_mod = Module.new

    if !opts[:sub]
      interface_mod.module_eval %{
        def #{name}(*args)
	  p = ::Fairy::user_level_filter(:#{name})
          raise "ユーザーレベルフィルタ(#{name})は定義されていません" unless p
	  p.call(@fairy, self, *args)			     
        end
      }
    else
      interface_mod.module_eval %{
        def #{name}(*args)
	  p = ::Fairy::user_level_filter(:#{name})
          raise "ユーザーレベルフィルタ(#{name})は定義されていません" unless p
  	  sub{|subf, input| p.call(subf, input, *args)}			     
        end
      }
    end
    Fairy.def_job_interface interface_mod
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
		   master_port = CONF.MASTER_PORT)
      @name2backend_class = {}

      @deep_connect = DeepConnect.start(0)
      @master_deepspace = @deep_connect.open_deepspace(master_host, master_port)
      @master = @master_deepspace.import("Master")

      @controller = @master.assgin_controller
      @controller.connect(self)

      @logger = @master.logger
      Log.type = "[C]"
      Log.pid = @controller.id
      Log.logger = @logger
      Log::info self, "fairy connected!!"

      @stdout_mutex = Mutex.new
    end

    def initialize_subfairy(fairy)
      @name2backend_class = {}
      @deep_connect = fairy.instance_eval{@deep_connect}
      @master_deepspace = fairy.instance_eval{@master_deepspace}
      @master = fairy.instance_eval{@master}

      @controller = @master.assgin_controller
      @controller.connect(self)
      
      # Logは親と共有される
      # なので, IDは親と同じになる(process idなので当たり前)
      
      @stdout_mutex = fairy.instance_eval{@stdout_mutex}
    end

    attr_reader :controller

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
      begin
	local_exp = exp.dc_deep_copy
      rescue
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

  def def_fairy_interface(mod)
    ::Fairy::Fairy.instance_eval{include mod}
  end
  module_function :def_fairy_interface

end

require "job/job"
require "job/input"

require "job/addins"

