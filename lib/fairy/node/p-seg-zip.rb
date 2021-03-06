# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"

require "fairy/node/p-io-filter"
require "fairy/node/p-single-exportable"

module Fairy
  class PSegZip<PSingleExportFilter
    Processor.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL, VAL)")

    def initialize(id, ntask, bjob, opts, block_source)
      super
      @block_source = block_source
#      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
#      @map_proc = @context.create_proc(@block_source)

      @zip_imports = nil
      @zip_imports_mutex = Mutex.new
      @zip_imports_cv = XThread::ConditionVariable.new
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

      policy = @opts[:prequeuing_policy]
      # 仮
      @zip_imports_mutex.synchronize do
	@zip_imports = zinputs.collect{|zinput| 
	  import = Import.new(policy)
	  import.no = zinput.no
	  import.add_key(zinput.key)
	  import.set_log_callback do |n, key| 
	    Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
	  end

	  import
	}
	@zip_imports_cv.broadcast
      end
      @zip_imports
    end
    DeepConnect.def_method_spec(self, :rets => "VAL", :method => :zip_inputs=, :args => "VAL")

    def basic_each(&block)
      @map_proc = BBlock.new(@block_source, @context, self)
      @input.each do |e|
	zips = zip_imports.collect{|import| import.pop}
	if Import::CTLTOKEN_NULLVALUE === (v = @map_proc.yield(e, *zips))
	  next
	end
	block.call v
      end
    end

#     def start
#       super do
# 	@map_proc = BBlock.new(@block_source, @context, self)
# 	@import.each do |e|
# 	  zips = zip_imports.collect{|import| import.pop}
# 	  @export.push @map_proc.yield(e, *zips)
# 	end
#       end
#     end
  end
end
