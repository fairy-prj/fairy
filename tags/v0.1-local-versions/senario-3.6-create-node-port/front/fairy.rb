
require "front/reference"
require "backend/controller"

module Fairy

  class Fairy

    def initialize
      @backend_controller = Controller.new
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
