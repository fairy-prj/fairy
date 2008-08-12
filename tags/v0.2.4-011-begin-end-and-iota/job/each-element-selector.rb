
require "job/filter"

module Fairy

  class EachElementSelector<Filter
    module Interface
      def select(block_source, opts = nil)
	raise "�֥�å��ϼ����դ����ޤ���" if block_given?
	mapper = EachElementSelector.new(@fairy, opts, block_source)
	mapper.input=self
	mapper
      end

      def grep(regexp, opts = nil)
	select(%{|e| /#{regexp.source}/ === e}, opts)
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BEachElementSelector"
    end
  end

end
