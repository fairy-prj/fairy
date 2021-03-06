# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"

require "fairy/client/io-filter"
require "fairy/share/port"

module Fairy
  class Here<IOFilter
    include Enumerable

    module Interface
      def here(opts = nil)
	here = Here.new(@fairy, opts)
	here.input= self
	here
      end
    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts = nil)
      super
    end

    def backend_class_name
      "CHere"
    end

#     def each(&block)
#       backend.each{|e| block.call e}
#     end

    def each(&block)
      policy = @opts[:prequeuing_policy]
      
      imports = XThread::Queue.new

      parent_thread = Thread.current
      
      Thread.start do
	begin
	  backend.each_node do |node|
	    node.start_export
	    import = Import.new(policy)
	    import.set_log_callback do |n, key| 
	      Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
	    end
	    import.no_import = 1
	    node.export.output = import
	    imports.push import
	    nil # 消すな!!(BUG#250対応)
	  end
	  imports.push nil
	rescue Exception
	  parent_thread.raise $!
	end
      end

      while imp = imports.pop
	imp.each do |e|
	  block.call e
	end
      end
    end

    def each_with_bjobeach(&block)
      backend.each_buf do |buf|
	buf.each &block
	# GCの問題
	buf = nil
      end
    end

    def to_a
      ary = []
      backend.each{|e| ary.push e}
      ary
    end

  end

#  class BHere;end
end
