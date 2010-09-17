# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "fairy/client/filter"

module Fairy

  class InputVArray < Filter

    def self.input(fairy, opts, varray)
      input_va = new(fairy, opts, varray)
      input_va.start
      input_va
    end
    
    def backend_class_name
      "BInputVArray"
    end

    def start
      backend.start
    end
  end
end


    
      
      
