
require "deep-connect/deep-connect.rb"

#DeepConnect::Organizer.immutable_classes.push Array


module Fairy

  class Fairy

    def initialize(master_host, master_port)
      @name2backend_class = {}

      @deep_connect = DeepConnect.start(0)
      @master_deepspace = @deep_connect.open_deepspace(master_host, master_port)
      @master = @master_deepspace.import("Master")

      @controller = @master.assgin_controller
      @controller.connect(self)
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

    def def_pool_variable(vname, value = nil)
      @controller.def_pool_variable(vname, value)
    end

    def pool_variable(vname, *value)
      @controller.pool_variable(vname, *value)
    end

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

