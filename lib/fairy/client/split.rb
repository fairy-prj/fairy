# encoding: UTF-8

require "fairy/client/io-filter"

module Fairy
  class Split<IOFilter
    module Interface
      def split(n, opts=nil)
	splitter = Split.new(@fairy, opts, n)
	splitter.input = self
	splitter
      end
    end
    Fairy::def_filter_interface Interface


    def initialize(fairy, opts, n)
      super
      @no_split = n
      @opts = opts
    end

    def backend_class_name
      "CSplit"
    end
  end
end
