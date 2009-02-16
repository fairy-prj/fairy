# encoding: UTF-8

require "share/exceptions"

module Fairy
  module BInputtable

    def initialize(*rests)
      super
      @input = nil

      @create_node_thread = nil
      @create_node_mutex = Mutex.new
    end

    def input=(input)
      @input = input
#      input.output = @input

      start_create_nodes
    end

    attr_reader :input

    def start_create_nodes
      Log::debug self, "START_CREATE_NODES: #{self}"
      @create_node_thread = Thread.start{
	create_nodes
      }
      nil
    end

    if RUBY_VERSION >= "1.9.0"
      # create_nodes init_njob: {|node| initialize of node}
      def create_nodes(opts = {})
	begin
	  no = 0
#	  @input.each_export do |export, node, opts|
	  @input.each_export do |export, node, opts={}|
	    @create_node_mutex.synchronize do
	      new_n = create_and_add_node(export, node)
	      no += 1
	      if opts[:init_njob]
		opts[:init_njob].call(new_n)
	      end
	    end
	  end

	rescue BreakCreateNode
	  # do nothing
	  Log::debug self, "CAUGHT EXCEPTION: BreakCreateNode: #{self}" 
	rescue Exception
	  Log::debug_exception(self)
	  raise
	ensure
	  Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	  self.number_of_nodes = no
	end
      end
    else
      def create_nodes(opts = {})
	begin
	  no = 0
	  @input.each_export do |export, node, opts|
	    opts = {} if opts.nil?
	    @create_node_mutex.synchronize do
	      new_n = create_and_add_node(export, node)
	      no += 1
	      if opts[:init_njob]
		opts[:init_njob].call(new_n)
	      end
	    end
	  end

	rescue BreakCreateNode
	  # do nothing
	  Log::debug self, "CAUGHT EXCEPTION: BreakCreateNode: #{self}" 
	rescue Exception
	  Log::debug_exception(self)
	  raise
	ensure
	  Log::debug self, "CREATE_NODES: #{self}.number_of_nodes=#{no}"
	  self.number_of_nodes = no
	end
      end
    end

    def create_and_add_node(input_export, input_node)
      node = nil
      @controller.assign_inputtable_processor(self, 
					      @input, 
					      input_node, 
					      input_export) do |processor|
	node = create_node(processor)
      end
      node.input= input_export
      input_export.output = node.import
      node
    end

    def break_running(njob = nil)
      super
      Thread.start{@input.break_running}
    end
  end
end
