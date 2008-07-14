
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
      puts "CREATE BJOB: #{self.class}"
      @controller = controller

      @number_of_nodes = nil
      @number_of_nodes_mutex = Mutex.new
      @number_of_nodes_cv = ConditionVariable.new

      @nodes = []
      @nodes_mutex = Mutex.new
      @nodes_cv = ConditionVariable.new

      @nodes_status = {}
      @nodes_status_mutex = Mutex.new
      @nodes_status_cv = ConditionVariable.new

      start_watch_node_status if watch_status?
    end

    def number_of_nodes
      @number_of_nodes_mutex.synchronize do
	while @number_of_nodes
	  @number_of_nodes_cv.wait(@number_of_nodes_mutex)
	end
	@number_of_nodes
      end
    end

    def number_of_nodes=(no)
      @number_of_nodes = no
      @number_of_nodes_cv.broadcast
      @nodes_cv.broadcast
    end

    def nodes
      @nodes_mutex.synchronize do
	@nodes
      end
    end

    def add_node(node)
      @nodes_mutex.synchronize do
	@nodes.push node
	@nodes_cv.broadcast
      end
    end

    def each_node(flag = nil, &block)
      if flag == :exist_only
	return each_node_exist_only &block
      end
      @nodes_mutex.synchronize do
	idx = 0
	while !@number_of_nodes || idx < @number_of_nodes
	  unless @nodes[idx]
	    @nodes_cv.wait(@nodes_mutex)
	    next
	  end
	  block.call @nodes[idx] 
	  idx +=1
	end
      end
    end

    def each_node_exist_only(&block)
      nodes = @nodes_mutex.synchronize{@nodes.dup}
      nodes.each &block
    end

    def each_export(&block)
      each_node do |node|
	puts "X1: #{self}"
	puts "X1#{node.inspect}"
#	puts "X1#{node.peer_inspect}"
	exp = node.export
puts "X2"
	block.call exp, node
	node.export.output.no_import = 1
      end
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

	all_finished = false
	while !@number_of_nodes || !all_finished
	  @nodes_status_mutex.synchronize do
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end

	  all_finished = @number_of_nodes
	  puts "Status Changed: BEGIN #{self}"
	  each_node(:exist_only) do |node|
	    st = @nodes_status[node]
	    puts "  node: #{node} status: #{st.id2name}" if st
	    STDOUT.flush
	    all_finished &&= st==:ST_FINISH
	  end
	  puts "Status Changed: END #{self}"
	end
	puts "  ALL NJOB finished"
      end
    end
  end
end
