
require "backend/binput"

module Fairy
  class BThere<BInput

    def initialize(controller, opts, enumerator)
      super
      @enumerator = enumerator
    end

    def node_class_name
      "NThere"
    end

    def start
      processor = @controller.assign_new_processor(self)
      nthere = processor.create_njob(node_class_name, self, @opts, @enumerator)
      add_node nthere
      self.number_of_nodes = 1
      nthere.start
    end
  end
end


      
