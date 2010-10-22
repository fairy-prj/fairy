# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "forwardable"

require "deep-connect"

require "fairy/master/c-io-filter"

module Fairy
  class CInject<CIOFilter
    extend Forwardable

    Controller.def_export self

    def initialize(controller, opts, block_source)
      super

      @block_source = block_source

      @clocal_inject = CLocalInject.new(controller, opts, block_source)
      @cwide_inject = CWideInject.new(controller, opts, block_source)

    end

    def_delegator :@cwide_inject, :value
    def_delegator :@cwide_inject, :output=
    def_delegator :@cwide_inject, :each_assigned_filter
    def_delegator :@cwide_inject, :each_export_by
    def_delegator :@cwide_inject, :bind_export
    

    def input=(input)
      @clocal_inject.input = input
      @cwide_inject.input = @clocal_inject
    end

    class CLocalInject<CIOFilter
      def initialize(controller, opts, block_source)
	super
	@block_source = block_source

	@no = 0
	@first_node = nil
	@first_node_mutex = Mutex.new
      end

      def node_class_name
	"PLocalInject"
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

    end

    class CWideInject<CIOFilter
      def initialize(controller, opts, block_source)
	super
	@block_source = block_source
      end

      def node_class_name
	"PWideInject"
      end

      def njob_creation_params
	[@block_source]
      end

      def value
	each_node{|node| return node.value}
      end
    end
  end
end
