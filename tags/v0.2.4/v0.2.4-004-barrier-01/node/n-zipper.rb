
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  class NZipper<NFilter
    include NSingleExportable

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, VAL)")

    def initialize(processor, bjob, opts, block_source)
      super(processor, bjob)
      @opts = opts
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
      @map_proc = @context.create_proc(@block_source)

      @zip_imports = nil
      @zip_imports_mutex = Mutex.new
      @zip_imports_cv = ConditionVariable.new
    end

    def zip_imports
      @zip_imports_mutex.synchronize do
	while !@zip_imports
	  @zip_imports_cv.wait(@zip_imports_mutex)
	end
      end
      @zip_imports
    end

    DeepConnect.def_method_spec(self, "VAL zip_imports")

    def zip_inputs=(zinputs)
      # 仮
      @zip_imports_mutex.synchronize do
	@zip_imports = zinputs.collect{|zinput| 
	  import = Import.new
	  import.no = zinput.no
	  import.add_key(zinput.key)
	  import
	}
	@zip_imports_cv.broadcast
      end
      @zip_imports
    end
    DeepConnect.def_method_spec(self, :rets => "VAL", :method => :zip_inputs=, :args => "VAL")

    def start
      super do
	@import.each do |e|
	  zips = zip_imports.collect{|import| import.pop}
	  @export.push @map_proc.call(e, *zips)
	end
      end
    end
  end
end
