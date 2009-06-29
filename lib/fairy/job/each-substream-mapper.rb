# encoding: UTF-8

require "fairy/job/filter"

module Fairy

  class EachSubStreamMapper<Filter
    module Interface
      def smap(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	block_source = BlockSource.new(block_source) 
	mapper = EachSubStreamMapper.new(@fairy, opts, block_source)
	mapper.input=self
	mapper
      end

      # emap(%{|enum| enum.collect{..})
      def emap(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	map_source = %{|i, o| proc{#{block_source}}.call(i).each{|e| o.push e}}
	smap(map_source, opts)
      end

      def map_flatten(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	map_source = %{|i, o| 
          ary = i.map{|*e| proc{#{block_source}}.call(*e)}
          if o.respond_to?(:push_buf)
             o.push_buf ary.flatten(#{opts[:N] if opts})
          else
             ary.each{|e| e.each{|ee| o.push ee}}
          end}
	smap(map_source, opts)
      end
      alias mapf map_flatten
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BEachSubStreamMapper"
    end
  end
end
