# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "uri"

require "fairy/controller"
require "fairy/master/c-input"
require "fairy/share/vfile"
require "fairy/share/file-place"

module Fairy
  class CInputFile<CInput
    Controller.def_export self

    def CInputFile.open(controller, opts, descripter)
      bfile = CInputFile.new(controller, opts)
      bfile.open(desctipter)
      bfile
    end
    DeepConnect.def_single_method_spec(self, "REF open(REF, VAL, VAL)")

    def node_class_name
      "PInputFile"
    end

    def open(vf)
      @vfile = vf
      @cfile_place = CFilePlace.new(@vfile)

      start_create_nodes
    end
    DeepConnect.def_method_spec(self, "REF open(DVAL)")

    def input
      @cfile_place
    end

#     def create_and_add_node(processor, mapper)
#       node = super
#       node.open(mapper.input.path)
#       node
#     end

  end
end
