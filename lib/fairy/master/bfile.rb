# encoding: UTF-8

require "uri"

require "fairy/controller"
require "fairy/backend/binput"
require "fairy/share/vfile"
require "fairy/share/file-place"

module Fairy
  class BFile<BInput
    Controller.def_export self

    def BFile.open(controller, opts, descripter)
      bfile = BFile.new(controller, opts)
      bfile.open(desctipter)
      bfile
    end
    DeepConnect.def_single_method_spec(self, "REF open(REF, VAL, VAL)")

    def node_class_name
      "NFile"
    end

    def open(vf)
      @vfile = vf
      @bfile_place = BFilePlace.new(@vfile)

      start_create_nodes
    end
    DeepConnect.def_method_spec(self, "REF open(DVAL)")

    def input
      @bfile_place
    end

#     def create_and_add_node(processor, mapper)
#       node = super
#       node.open(mapper.input.path)
#       node
#     end

  end
end
