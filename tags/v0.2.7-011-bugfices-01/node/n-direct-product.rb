
require "node/n-filter"
require "node/n-single-exportable"

module Fairy
  module NDirectProduct
    class NPreFilter<NFilter
      Processor.def_export self

      def initialize(processor, bjob, opts, block_source)
	super
	@block_source = block_source

	@exports = nil
	@exports_mutex = Mutex.new
	@exports_cv = ConditionVariable.new
      end

      def input=(input)
	super
	start_watch_exports
      end	

#       def number_of_exports=(n)
# 	@exports_mutex.synchronize do
# 	  @exports = []
# 	  n.times{@exports.push Export.new}
# 	end
# 	@exports_cv.broadcast
#       end

      def start_watch_exports
	Thread.start do
	  n = @bjob.number_of_exports
	  @exports_mutex.synchronize do
	    @exports = []
	    n.times do
	      exp = Export.new
	      exp.no = @import.no
	      exp.add_key(@import.key)
	      exp.output_no_import = 1
	      @exports.push exp
	    end
	  end
	  @exports_cv.broadcast
	end
      end

      def exports
	@exports_mutex.synchronize do
	  while @exports.nil?
	    @exports_cv.wait(@exports_mutex)
	  end
	end
	@exports
      end
      
      def start
	super do
	  @import.each do |e|
	    exports.each{|exp| exp.push e}
	  end
	  exports.each{|exp| exp.push :END_OF_STREAM}
	end
      end
    end

    class NPostFilter<NSingleExportFilter
      Processor.def_export self

      def initialize(processor, bjob, opts, block_source)
	super
	@block_source = block_source
	
	@other_imports = nil
	@other_imports_mutex = Mutex.new
	@other_imports_cv = ConditionVariable.new
      end

      def other_inputs=(exports)
	@other_imports_mutex.synchronize do
	  @other_imports = exports.collect{|exp|
	    imp = Import.new
	    imp.no = exp.no
	    imp.add_key(exp.key)
	    imp.no_import = 1
	    exp.output = imp
	    imp
	  }
	  @other_imports_cv.broadcast
	end
      end

      def other_imports
	@other_imports_mutex.synchronize do
	  while !@other_imports
	    @other_imports_cv.wait(@other_imports_mutex)
	  end
	  @other_imports
	end
      end

      def start
	super do
	  @map_proc = BBlock.new(@block_source, @context, self)

	  elements = []
	  elements.push @import.to_a
	  elements.push *other_imports.collect{|i| i.to_a}

	  idxs = elements.collect{|e| 0}
	  max_idxs = elements.collect{|e| e.size}

	  cont = true
	  while cont
	    e = elements.zip(idxs).collect{|ary, idx| ary[idx]}
	    @export.push @map_proc.yield *e

	    (idxs.size-1).downto(0) do |idx|
	      idxs[idx] += 1
	      break if idxs[idx] < max_idxs[idx]
	      idxs[idx] = 0
	      cont = false if idx == 0 && idxs[idx] == 0
	    end
	  end
	end
      end

#       def start_org
# 	super do
# 	  @map_proc = BBlock.new(@block_source, @context, self)
# 	  products = @import.to_a.product(*other_imports.collect{|i| i.to_a})
# 	  products.each do |*e|
# 	    @export.push @map_proc.yield(*e)
# 	  end
# 	end
#       end
    end
  end
end