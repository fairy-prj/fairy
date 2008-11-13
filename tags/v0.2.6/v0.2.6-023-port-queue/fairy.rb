
require "thread"
require "deep-connect/deep-connect.rb"

require "share/conf"

#DeepConnect::Organizer.immutable_classes.push Array


module Fairy

  class Fairy

    def initialize(master_host = CONF.MASTER_HOST, 
		   master_port = CONF.MASTER_PORT)
      @name2backend_class = {}

      @deep_connect = DeepConnect.start(0)
      @master_deepspace = @deep_connect.open_deepspace(master_host, master_port)
      @master = @master_deepspace.import("Master")

      @controller = @master.assgin_controller
      @controller.connect(self)

      @stdout_mutex = Mutex.new
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

