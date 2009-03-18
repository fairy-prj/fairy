# encoding: UTF-8

require "job/filter"

module Fairy

  class EachElementSelector<Filter
    module Interface
      def select(block_source, opts = nil)
	raise "ブロックは受け付けられません" if block_given?
	block_source = BlockSource.new(block_source) 
	mapper = EachElementSelector.new(@fairy, opts, block_source)
	mapper.input=self
	mapper
      end

      alias find_all select

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
