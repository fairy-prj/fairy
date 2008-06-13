
require "front/reference"

module Fairy
  class BJob

    @@watch_status = false
    def self.watch_status
      @@watch_status
    end

    def self.watch_status=(val)
      @@watch_status=val
    end

    def initialize(controller)
      @controller = controller
      @nodes = Reference.new

      @nodes_status = {}
      @nodes_status_mutex = Mutex.new
      @nodes_status_cv = ConditionVariable.new

      start_watch_node_status if watch_status?
    end

    def nodes
      @nodes.value
    end

    def nodes=(val)
      @nodes.value = val
    end

    def wait_node_arrived
      @nodes.wait_arrived
    end

    def update_status(node, st)
#      @nodes_status_mutex.synchronize do
	@nodes_status[node] = st
	@nodes_status_cv.broadcast
#      end
    end

    def watch_status?
      @@watch_status
    end

    def start_watch_node_status
      Thread.start do
	self.wait_node_arrived

	all_finished = false
	while !all_finished
	  @nodes_status_mutex.synchronize do
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end

	  all_finished = true
	  puts "Status Changed: #{self}"
	  self.nodes.each do |node|
	    st = @nodes_status[node]
	    puts "  node: #{node} status: #{st.id2name}" if st
	    STDOUT.flush
	    all_finished &= st==:ST_FINISH
	  end
	end
	puts "  ALL NJOB finished"
      end
    end
    def start_watch_node_status0
      Thread.start do
	self.wait_node_arrived

	all_finished = false
	@nodes_status_mutex.synchronize do
	  while !all_finished
	    @nodes_status_cv.wait(@nodes_status_mutex)

	    all_finished = true
	    puts "Status Changed: #{self}"
	    self.nodes.each do |node|
	      st = @nodes_status[node]
	      puts "  node: #{node} status: #{st.id2name}" if st
	      STDOUT.flush
	      all_finished &= st==:ST_FINISH
	    end
	  end
	end
	puts "  ALL NJOB finished"
      end
    end
  end
end
