
require "job/filter"
require "share/block-source"

module Fairy

  class EachElementMapper<Filter
    module Interface
      def map(block_source, opts = nil)
	raise "�֥�å��ϼ����դ����ޤ���" if block_given?
	block_source = BlockSource.new(block_source) 
	mapper = EachElementMapper.new(@fairy, opts, block_source)
	mapper.input=self
	mapper
      end
      alias collect map
     
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BEachElementMapper"
    end
  end
end
