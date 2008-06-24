
require "deep-connect/deep-connect.rb"

require "front/reference"
require "backend/controller"

module Fairy

  class Fairy

    def initialize(my_port, master_host, master_port)
      @deep_connect = DeepConnect.start(my_port)
      @session = @deep_connect.open_session(master_host, master_port)

      @backend_controller = @session.get_service("Controller")
    end

    attr_reader :backend_controller

    def input(ffile_descripter)
      FFile.input(self, ffile_descripter)
    end

    def send_atom(atom)
      ref = Reference.new
      Thread.start do
	ref.value = @backend_controller.send_atom(atom)
      end
      ref
    end
  end
end

require "job/ffile"
