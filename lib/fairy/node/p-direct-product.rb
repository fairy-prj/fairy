# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
require "fairy/node/p-single-exportable"

module Fairy
  module PDirectProduct
    class PPreFilter<PIOFilter
      Processor.def_export self

      def initialize(id, ntask, bjob, opts, block_source)
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

# 後で検討する
#	      policy = @opts[:postqueuing_policy]
#	      exp = Export.new(policy)
	      exp = Export.new
	      exp.njob_id = @id
	      exp.no = @input.no
	      exp.add_key(@input.key)
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
      
      def start_export
	start do
	  begin
	    @input.each do |e|
	      exports.each{|exp| exp.push e}
	    end
	  ensure
	    exports.each{|exp| exp.push :END_OF_STREAM}
	  end
 	end
       end
    end

    class PPostFilter<PSingleExportFilter
      Processor.def_export self

      def initialize(processor, bjob, opts, block_source)
	super
	@block_source = block_source
	
	@other_imports = nil
	@other_imports_mutex = Mutex.new
	@other_imports_cv = ConditionVariable.new
      end

      def input=(input)
	@input = input
#	self.no = input.no
	self.key = input.key
      end

      def other_inputs=(exports)
	@other_imports_mutex.synchronize do
	  @other_imports = exports.collect{|exp|
# 後で検討する
#           policy = @opts[:prequeuing_policy]
#	    imp = Import.new(policy)
	    imp = Import.new
	    imp.no = exp.no
	    imp.add_key(exp.key)
	    imp.no_import = 1
	    imp.set_log_callback do |n, key| 
	      Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
	    end

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

      def basic_each(&block)
	@map_proc = BBlock.new(@block_source, @context, self)

	elements = []
	elements.push @input.to_a
	elements.push *other_imports.collect{|i| i.to_a}

	idxs = elements.collect{|e| 0}
	max_idxs = elements.collect{|e| e.size}

	cont = true
	while cont
	  e = elements.zip(idxs).collect{|ary, idx| ary[idx]}
	  block.call @map_proc.yield *e

	  (idxs.size-1).downto(0) do |idx|
	    idxs[idx] += 1
	    break if idxs[idx] < max_idxs[idx]
	    idxs[idx] = 0
	    cont = false if idx == 0 && idxs[idx] == 0
	  end
	end
      end
    end
  end
end
