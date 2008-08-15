
require "deep-connect/deep-connect"

require "node/nfile"
require "node/nhere"
require "node/n-each-element-mapper"
require "node/n-each-element-selector"
require "node/n-each-substream-mapper"
require "node/n-group-by"
require "node/n-zipper"

module Fairy

  class Processor
    def initialize(id)
      @id = id

      @njobs = []
    end

    attr_reader :id

    def start(node_port, service=0)
      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Processor", self)

      @node_deepspace = @deepconnect.open_deepspace("localhost", node_port)
      @node = @node_deepspace.import("Node")

      @node.register_processor(self)
    end

    def no_njobs
      @njobs.size
    end


    def nfile_open(bfile, fn)
      nfile = NFile.open(self, bfile, fn)
      @njobs.push nfile
      nfile
    end

    def create_njob(njob_class_name, bjob, *opts)
#      opts = opts.to_a unless opts.empty?
      # �����ե��ޥ���
      klass = eval(njob_class_name)
#      njob = klass.new(self, bjob, *opts)
      njob = klass.new(self, bjob, *opts)
      @njobs.push njob
      njob
    end

    DeepConnect.def_method_spec(self, "REF create_njob(VAL, REF, *VAL)")

  end

  def Processor.start(id, controller_port)
    processor = Processor.new(id)
    processor.start(controller_port)
  end

end