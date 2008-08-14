
require "controller"
require "backend/binput"

module Fairy
  class BThere<BInput
    Controller.def_export self

    def initialize(controller, opts, enumerable)
      super
      @enumerable = enumerable
    end

    def node_class_name
      "NThere"
    end

    def start
      processor = @controller.assign_new_processor(self)
      nthere = processor.create_njob(node_class_name, self, @opts, @enumerable)
      add_node nthere
      self.number_of_nodes = 1
      nthere.start
    end
  end
end


      
