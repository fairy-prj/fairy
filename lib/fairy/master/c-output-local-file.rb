# encoding: UTF-8

require "fairy/backend/bhere"

module Fairy
  class BLFileOutput<BHere
    Controller.def_export(self)

    def node_class_name
      "NLFileOutput"
    end

    def output(filename)
      @filename = filename
      backend.output(self)
    end

  end
end
