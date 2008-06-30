
require "deep-connect/deep-connect.rb"
require "node/nfile"

module Fairy

  class Processor
    def initialize(id)
      @id = id
    end

    attr_reader :id

    def start(controller_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Processor", self)

      @controller_session = @deepconnect.open_session("localhost", controller_port)
      @controller = @controller_session.get_service("Controller")

      @controller.register_processor(self)
    end

    def NFile
      NFile
    end
  end

  def Processor.start(id, controller_port)
    processor = Processor.new(id)
    processor.start(controller_port)
  end

end
