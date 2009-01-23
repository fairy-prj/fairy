
require "deep-connect/deep-connect.rb"

require "front/reference"
require "backend/controller"

module Fairy

  class Fairy

    def initialize(master_host, master_port)
      #
      # BEGIN DFRQ
      # * 最初の立ち上げは?
      # * コントーローラが欲しい
      # * ここ周りのインターフェース
      #
      @deep_connect = DeepConnect.start(0)
      @session = @deep_connect.open_session(master_host, master_port)

      @backend_controller = @session.get_service("Controller")
      @name2backend_class = {}
      #
      # END DFRQ
      #
    end

    attr_reader :backend_controller

    def name2backend_class(backend_class_name)
      if klass = @name2backend_class[backend_class_name]
	return klass 
      end
      
      if klass =  @session.get_service(backend_class_name)
	@name2backend_class[backend_class_name] = klass
      end
      klass
    end

    def input(ffile_descripter)
      FFile.input(self, ffile_descripter)
    end

#     def send_atom(atom)
#       ref = Reference.new
#       Thread.start do
# 	ref.value = @backend_controller.send_atom(atom)
#       end
#       ref
#     end
  end
end

require "job/ffile"
