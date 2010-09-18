# encoding: UTF-8

require "fairy/master/c-here"

module Fairy
  class COutputLocalFile<CHere
    Controller.def_export(self)

    def node_class_name
      "POutputLocalFile"
    end

    def output(filename)
      @filename = filename
      backend.output(self)
    end

  end
end
