

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

    def create_and_start_nodes
      begin
	offset = 0
	offset = @opts[:offset] if @opts[:offset]
	split_no = @opts[:SPLIT_NO]

	first = offset
	no = 0
	split_no.times do
	  @create_node_mutex.synchronize do
	    no += 1
puts "NO: #{no}"
	    last = [first + @last.div(split_no), @last].min
	    @controller.assign_processor(self, :NEW_PROCESSOR) do |processor|
	      njob = create_node(processor, first, last)
	      njob.start
	      first = last + 1
	    end
	  end
	  sleep 0.1
	end
      rescue BreakCreateNode
	# do nothing
	puts "BREAK CREATE NODE: #{self}" 
      ensure
	self.number_of_nodes = no
      end
    end
  end
end
