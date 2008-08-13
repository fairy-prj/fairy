
require "job/filter"

module Fairy

  class EachElementMapper<Filter
    module Interface
      def map(block_source, opts = nil)
	raise "�֥��å��ϼ����դ����ޤ���" if block_given?
	mapper = EachElementMapper.new(@fairy, block_source)
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
      "BEachElementMapper"
    end
  end
end