
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

    def njob_creation_params
      [@enumerable]
    end

    def start
      nthere = nil
      @controller.assign_new_processor(self) do |processor|
	nthere = create_node(processor)
      end
      self.number_of_nodes = 1
      nthere.start
    end
  end
end


      
