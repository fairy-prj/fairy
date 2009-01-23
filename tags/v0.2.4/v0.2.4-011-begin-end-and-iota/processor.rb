
require "deep-connect/deep-connect"

require "node/nfile"
require "node/n-local-file-input"
require "node/n-input-iota"

require "node/n-file-output"
require "node/n-local-file-output"

require "node/nhere"
require "node/n-each-element-mapper"
require "node/n-each-element-selector"
require "node/n-each-substream-mapper"
require "node/n-group-by"
require "node/n-zipper"
require "node/n-splitter"
require "node/n-barrier"

module Fairy

  class Processor
    def initialize(id)
      @id = id

      @njobs = []
    end

    attr_reader :id

    def start(node_port, service=0)
      @addr = nil

      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Processor", self)

      @node_deepspace = @deepconnect.open_deepspace("localhost", node_port)
      @node = @node_deepspace.import("Node")

      @node.register_processor(self)
    end

    attr_accessor :addr

    def no_njobs
      @njobs.size
    end


    def nfile_open(bfile, opts, fn)
      nfile = NFile.open(self, bfile, opts, fn)
      @njobs.push nfile
      nfile
    end
    DeepConnect.def_method_spec(self, "REF nfile_open(REF, VAL, VAL)")

    def create_njob(njob_class_name, bjob, opts, *rests)
#      opts = opts.to_a unless opts.empty?
      # この辺イマイチ
      klass = eval(njob_class_name)
#      njob = klass.new(self, bjob, *opts)
      njob = klass.new(self, bjob, opts, *rests)
      @njobs.push njob
      njob
    end

    DeepConnect.def_method_spec(self, "REF create_njob(VAL, REF, VAL, *VAL)")

  end

  def Processor.start(id, controller_port)
    processor = Processor.new(id)
    processor.start(controller_port)
  end

end
