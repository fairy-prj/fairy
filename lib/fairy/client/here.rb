# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

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
      
      imports = Queue.new
      
      Thread.start do
Log::debug(self, "EACH_NODE: S")
	backend.each_node do |node|
Log::debug(self, "EACH_NODE: 0 #{node.id}")
	  node.start_export
Log::debug(self, "EACH_NODE: 1 #{node.id}")
	  import = Import.new(policy)
Log::debug(self, "EACH_NODE: 2 #{node.id}")
	  import.set_log_callback do |n, key| 
	    Log::verbose(self, "IMPORT POP key=#{key}: #{n}")
	  end
Log::debug(self, "EACH_NODE: 3 #{node.id}")
	  import.no_import = 1
Log::debug(self, "EACH_NODE: 4 #{node.id}")
	  node.export.output = import
Log::debug(self, "EACH_NODE: 5 #{node.id}")
	  imports.push import
Log::debug(self, "EACH_NODE: 6 #{node.id}")
	  nil # 消すな!!(BUG#250対応)
	end
Log::debug(self, "EACH_NODE: E ")
	imports.push nil
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
