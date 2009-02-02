# encoding: UTF-8

module Fairy

  class InputVArray < Job

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


    
      
      
