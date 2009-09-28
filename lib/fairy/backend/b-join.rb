# encoding: UTF-8

require "fairy/backend/b-filter"
require "fairy/backend/b-inputtable"

module Fairy
  class BJoin<BFilter
    Controller.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, REF)")

    def initialize(controller, opts, others, block_source)
      super
      @others = others
      @block_source = block_source

      @exports = {}
      @others_status = {}
      @exports_mutex = Mutex.new
      @exports_cv = ConditionVariable.new

      @key_proc
      init_key_proc
    end

    def init_key_proc
      case join_by
      when :ORDER
	@key_proc = proc{|input| input.no}
      when :KEY
	@key_proc = proc{|input| input.key}
      else
	ERR::Raise ERR::UnrecoginizedOption, join_by
      end
    end

    def join_by
      by = @opts[:by]
      return :ORDER unless by
      by.to_s.upcase.intern
    end

    def node_class_name
      "NJoin"
    end

    def njob_creation_params
      [@block_source]
    end

    def start_create_nodes
      Log::debug self, "START_CREATE_NODES: #{self}"
      @others.each do |other|
	Thread.start do
	  other.each_assigned_filter do |input_filter|
	    @exports_mutex.synchronize do
	      unless exps = @exports[@key_proc.call(input_filter)]
		exps = @exports[@key_proc.call(input_filter)] = {}
		@exports_cv.broadcast
	      end
	      exp = input_filter.start_export
	      exps[other] = exp
	    end
	  end
	  @exports_mutex.synchronize do
	    @others_status[other] = true
	    @exports_cv.broadcast
	  end
	end
      end
      super
    end

    def create_and_add_node(processor, mapper, opts={})
      node = create_node(processor) {|node|
	if opts[:init_njob]
	  opts[:init_njob].call(node)
	end
	mapper.bind_input(node)

	key = @key_proc.call(node)
	exps = nil
	@exports_mutex.synchronize do
	  while !(exps = other_filter_of(key))
	    @exports_cv.wait(@exports_mutex)
	  end
	end
 	node.join_inputs = exps
 	exps.zip(node.join_imports) do |other, import|
	  other.output = import
	  import.no_import = 1
	end
      }
      node
    end

    class NoAllFilter<Exception;end

    def other_filter_of(key)
      begin
	@others.collect do |o| 
	  unless exp = @exports[key][o]
	    unless @other_status[o]
	      raise NoAllFilter
	    end
	  end
	  exp
	end
      rescue NoAllFilter
	return nil
      rescue
	return nil
      end
    end

    def break_running
      super
      @others.each{|other| Thread.start{other.break_running}}
    end

    class BPreJoinedFilter<BFilter
      Controller.def_export self

      def initialize(controller, opts)
	super
      end

      def node_class_name
	"NIdentity"
      end

      def njob_creation_params
	[]
      end
    end

  end
end
