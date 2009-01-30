
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  class NJoin<NSingleExportFilter
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      @block_source = block_source
      @join_imports = nil
      @join_imports_mutex = Mutex.new
      @join_imports_cv = ConditionVariable.new
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
      @join_imports_mutex.synchronize do
	@join_imports = jinputs.collect{|jinput| 
	  if jinput
	    import = Import.new
	    import.no = jinput.no
	    import.add_key(jinput.key)
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

    def start
      super do
	@map_proc = BBlock.new(@block_source, @context, self)
	arg = [@import]
	arg.push *join_imports
	arg.push @export
	  
Log::debug(self, "START")
#Log::debug(self, @block_source.source)
	@map_proc.yield(@import, *join_imports, @export)
Log::debug(self, "END")
      end
    end
  end
end
