
require "deep-connect/deep-connect"

# require "node/nfile"
# require "node/n-local-file-input"
# require "node/n-input-iota"
# require "node/n-there"

# require "node/n-file-output"
# require "node/n-local-file-output"

# require "node/nhere"
# require "node/n-each-element-mapper"
# require "node/n-each-element-selector"
# require "node/n-each-substream-mapper"
# require "node/n-group-by"
# require "node/n-zipper"
# require "node/n-splitter"
# require "node/n-barrier"

module Fairy

  class Processor

    EXPORTS = []
    def Processor.def_export(obj, name = nil)
      unless name
	if obj.kind_of?(Class)
	  if /Fairy::(.*)$/ =~ obj.name
	    name = $1
	  else
	    name = obj.name
	  end
	else
	  raise "���饹�ʳ�����Ͽ����Ȥ��ˤϥ����ӥ�̾��ɬ�פǤ�(%{obj})"
	end
      end

      EXPORTS.push [name, obj]
    end


    def initialize(id)
      @id = id

      @services = {}

      @njobs = []
    end

    attr_reader :id

    def start(node_port, service=0)
      @addr = nil

      @deepconnect = DeepConnect.start(service)
      @deepconnect.register_service("Processor", self)

      for name, obj in EXPORTS
	export(name, obj)
      end

      @node_deepspace = @deepconnect.open_deepspace("localhost", node_port)
      @node = @node_deepspace.import("Node")

      @node.register_processor(self)
    end

    attr_accessor :addr

    def export(service, obj)
      @services[service] = obj
    end

    def import(service)
      svs = @services[service]
      unless svs
	raise "�����ӥ�(#{service})����Ͽ����Ƥ��ޤ���"
      end
      svs
    end

    def no_njobs
      @njobs.size
    end

#     def nfile_open(bfile, opts, fn)
#       nfile = NFile.open(self, bfile, opts, fn)
#       @njobs.push nfile
#       nfile
#     end
#     DeepConnect.def_method_spec(self, "REF nfile_open(REF, VAL, VAL)")

    def create_njob(njob_class_name, bjob, opts, *rests)
      klass = import(njob_class_name)
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

require "node/addins"