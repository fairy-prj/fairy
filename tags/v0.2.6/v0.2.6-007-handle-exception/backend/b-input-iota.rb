

module Fairy

  class BIota<BInput
    Controller.def_export self

    def initialize(controller, opts, last)
      super
      @last = last - 1
    end

    def node_class_name
      "NIota"
    end

    def start
      offset = 0
      offset = @opts[:offset] if @opts[:offset]
      split_no = @opts[:SPLIT_NO]

      self.number_of_nodes = split_no
      first = offset
      split_no.times do 
	last = [first + @last.div(split_no), @last].min
	@controller.assign_processor(self, :NEW_PROCESSOR) do |processor|
	  njob = create_node(processor, first, last)
	  njob.start
	  first = last + 1
	end
      end
    end
  end
end