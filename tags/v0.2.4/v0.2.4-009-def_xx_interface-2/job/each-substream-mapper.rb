
require "job/filter"

module Fairy

  class EachSubStreamMapper<Filter
    module Interface
      def smap(block_source, opts = nil)
	raise "ブロックは受け付けられません" if block_given?
	mapper = EachSubStreamMapper.new(@fairy, block_source)
	mapper.input=self
	mapper
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, block_source, opts=nil)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "BEachSubStreamMapper"
    end
  end
end
