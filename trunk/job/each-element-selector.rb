
require "job/filter"

module Fairy

  class EachElementSelector<Filter
    module Interface
      def select(block_source, opts = nil)
	raise "ブロックは受け付けられません" if block_given?
	mapper = EachElementSelector.new(@fairy, block_source)
	mapper.input=self
	mapper
      end

      def grep(regexp, opts = nil)
	select %{|e| /#{regexp.source}/ === e}
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, block_source, opts=nil)
      super
      @block_source = block_source
      @opts = opts
    end

    def backend_class_name
      "BEachElementSelector"
    end
  end

end
