# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "forwardable"

require "deep-connect/deep-connect"

require "fairy/master/c-io-filter"

module Fairy
  class CFind<CIOFilter
    extend Forwardable

    Controller.def_export self

    def initialize(controller, opts, block_source)
      super

      @block_source = block_source

      @clocal_find = CLocalFind.new(controller, opts, block_source)
      @cfind_result = CFindResult.new(controller, opts, self)

    end

    def_delegator :@cfind_result, :value
    def_delegator :@cfind_result, :output=

    def input=(input)
      @clocal_find.input = input
      @cfind_result.input = @clocal_find
    end

    def update_find
      @clocal_find.break_running
    end

    class CLocalFind<CIOFilter
      def initialize(controller, opts, block_source)
	super
	@block_source = block_source

	@no = 0
	@first_node = nil
	@first_node_mutex = Mutex.new
      end

      def node_class_name
	"PLocalFind"
      end

      def njob_creation_params
	[@block_source]
      end
      
      def each_assigned_filter(&block)
	super

	@first_node.export.output_no_import = @no
      end

      def each_export_by(njob, mapper, &block)
	@first_node_mutex.synchronize do
	  @no += 1
	  if @first_node
	    njob.export.output = @first_node.export.output
	    njob.export.no = @no - 1
	  else
	    super
	    @first_node = njob
	  end
	end
      end

      def bind_export(exp, imp)
	# do nothing.
      end

#       def each_export(&block)
# 	no = 0
# 	first_node = nil
# 	each_node do |node|
# 	  no += 1
# 	  if first_node
# 	    node.export.output = first_node.export.output
# 	  else
# 	    first_node = node
# 	    block.call node.export, node
# 	  end
# 	end
# 	first_node.export.output_no_import = no
#       end

#       def find_break
# 	# create node 中ならそれをとめる
# 	break_create_node
# 	# 各tasklettをとめる
# 	each_node do |tasklet|
# 	  tasklet.find_break
# 	end
#       end

    end

    class CFindResult<CIOFilter
      def initialize(controller, opts, bfind)
	super
	@bfind = bfind

	@find_mutex = Mutex.new
	@findp = false
      end

      def node_class_name
	"PFindResult"
      end

      def njob_creation_params
	[]
      end

      def value
	each_node{|node| return node.value}
      end

      def update_find
	@find_mutex.synchronize do
	  if !@findp
	    @findp = true
	    @bfind.update_find
	  end
	end
      end
    end
  end
end
