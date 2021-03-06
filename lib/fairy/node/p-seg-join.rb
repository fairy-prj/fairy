# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"

require "fairy/node/p-io-filter"
require "fairy/node/p-single-exportable"

module Fairy
  class PSegJoin<PSingleExportFilter
    Processor.def_export self

    def initialize(id, ntask, bjob, opts, block_source)
      super
      @block_source = block_source

      @join_imports = nil
      @join_imports_mutex = Mutex.new
      @join_imports_cv = XThread::ConditionVariable.new
    end

    def join_imports
      @join_imports_mutex.synchronize do
	while !@join_imports
	  @join_imports_cv.wait(@join_imports_mutex)
	end
      end
      @join_imports
    end

    DeepConnect.def_method_spec(self, "VAL join_imports")

    def join_inputs=(jinputs)
      policy = @opts[:prequeuing_policy]
      @join_imports_mutex.synchronize do
	@join_imports = jinputs.collect{|jinput| 
	  if jinput
	    import = Import.new(policy)
	    import.no = jinput.no
	    import.add_key(jinput.key)
	    import.set_log_callback do |n, key| 
	      Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
	    end

	    import
	  else
	    nil
	  end
	}
	@join_imports_cv.broadcast
      end
      @join_imports
    end

    DeepConnect.def_method_spec(self, :rets => "VAL", :method => :join_inputs=, :args => "VAL")

    def basic_each(&block)
      @map_proc = BBlock.new(@block_source, @context, self)
      arg = [@input]
      arg.push *join_imports
      arg.push block
	  
      @map_proc.yield(*arg)
    end


#     def start
#       super do
# 	@map_proc = BBlock.new(@block_source, @context, self)
# 	arg = [@import]
# 	arg.push *join_imports
# 	arg.push @export
	  
# Log::debug(self, "START")
# #Log::debug(self, @block_source.source)
# #	@map_proc.yield(@import, *join_imports, @export)
# 	@map_proc.yield(*arg)
# Log::debug(self, "END")
#       end
#     end
  end
end
