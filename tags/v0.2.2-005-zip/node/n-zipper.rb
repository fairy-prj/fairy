
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  class NZipper<NFilter
    include NSingleExportable

    def initialize(processor, bjob, opts, block_source)
      super(processor, bjob)
      @opts = opts.to_a
      @block_source = block_source
      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)

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

    def zip_inputs=(zinputs)
      # ²¾
      @zip_imports_mutex.synchronize do
	zinputs = zinputs.to_a

	@zip_imports = zinputs.collect{|zinput| 
	  import = Import.new
	  import.add_key(zinput.key)
	  import
	}
	@zip_imports_cv.broadcast
      end
      @zip_imports
    end

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
