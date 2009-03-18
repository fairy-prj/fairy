# encoding: UTF-8

require "job/filter"

module Fairy
  class Splitter<Filter
    module Interface
      def split(n, opts=nil)
	splitter = Splitter.new(@fairy, opts, n)
	splitter.input = self
	splitter
      end
    end
    Fairy::def_job_interface Interface


    def initialize(fairy, opts, n)
      super
      @no_split = n
      @opts = opts
    end

    def backend_class_name
      "BSplitter"
    end
  end
end
