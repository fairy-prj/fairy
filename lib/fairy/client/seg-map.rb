# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/io-filter"

module Fairy

  class SegMap<IOFilter
    module Interface

      def smap(block_source, opts = nil)
	raise "No compatibility after fairy-0.5"
      end

      def smap2(block_source, opts = nil)
	raise "No compatibility after fairy-0.6"
      end

      def seg_map(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	block_source = BlockSource.new(block_source) 
	mapper = SegMap.new(@fairy, opts, block_source)
	mapper.input=self
	mapper
      end

      # emap(%{|input| input.collect{..})
      def emap(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	map_source = %{|i, block| proc{#{block_source}}.call(i).each{|e| block.call e}}
	seg_map(map_source, opts)
      end

      def map_flatten(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	map_source = %{|i, block|
          i.each do |e|
            enum = proc{#{block_source}}.call(e)
            enum.each do |f|
              #{n = opts && opts[:N]; n ||= 1
                case n
                when 1
                  "block.call f"
                when 2
                  "if f.respond_to?(:each)
                     f.each{|g| block.call(g)}
                   else
                     block.call f
                   end"
                else
                 "if f.respond_to?(:flatten)
                    f.flatten(#{opts[:N]} - 2).each{|g| block.call(g)}
                  else
                    block.call f
                  end"
                end}
            end
          end
        }
	seg_map(map_source, opts)
      end
      alias mapf map_flatten

    end
    Fairy::def_filter_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "CSegMap"
    end
  end
end
