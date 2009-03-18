# encoding: UTF-8

require "job/filter"

module Fairy
  class Inject<Filter

    module Interface
      # filter.inject(%{...}, :init_value = val)
      def inject(block_source, opts = nil)
	block_source = BlockSource.new(block_source) 
	inject = Inject.new(@fairy, opts, block_source)
	inject.input = self
	#DeepConnect::future{inject.value}
	inject
      end

      def min(block_source=nil, opts = nil)
	unless block_source
	  block_source = %{|e1, e2| e1 <=> e2}
	end
	
	inject(%{|r, v| (proc{#{block_source}}.call(r, v) < 0) ? r : v})
      end

      def min_by(block_source, opts = nil)
	pair = map(%{|v| [proc{#{block_source}}.call(v), v]})
	min_by = pair.inject(%{|r, v| ((r[0] <=> v[0]) < 0) ? r : v})
	def min_by.value
	  super[1]
	end
	min_by
      end

      def max(block_source=nil, opts = nil)
	unless block_source
	  block_source = %{|e1, e2| e1 <=> e2}
	end
	
	inject(%{|r, v| (proc{#{block_source}}.call(r, v) < 0) ? v : r})
      end

      def max_by(block_source, opts = nil)
	pair = map(%{|v| [proc{#{block_source}}.call(v), v]})
	max_by = pair.inject(%{|r, v| ((r[0] <=> v[0]) < 0) ? v : r})
	def max_by.value
	  super[1]
	end
	max_by
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BInject"
    end

    def value
      backend.value
    end
  end
end
