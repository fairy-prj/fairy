
require "deep-connect/deep-connect"

require "node/nfile"
require "node/nhere"
require "node/n-each-element-mapper"
require "node/n-each-element-selector"
require "node/n-each-substream-mapper"
require "node/n-group-by"

module Fairy

  class Processor
    def initialize(id)
      @id = id
    end

    attr_reader :id

    def start(controller_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Processor", self)

      @controller_deepspace = @deepconnect.open_deepspace("localhost", controller_port)
      @controller = @controller_deepspace.import("Controller")

      @controller.register_processor(self)
    end

    def nfile_open(bfile, fn)
      nfile = NFile.open(self, bfile, fn)
    end

    def create_njob(njob_class_name, bjob, *opts)
      opts = opts.to_a unless opts.empty?
      # この辺イマイチ
      klass = eval(njob_class_name)
#      njob = klass.new(self, bjob, *opts)
      njob = klass.new(self, bjob, *opts)
      njob
    end

  end

  def Processor.start(id, controller_port)
    processor = Processor.new(id)
    processor.start(controller_port)
  end

end
