
require "job/filter"

module Fairy
  class Zipper<Filter
    ZIP_BY_SUBSTREAM = :ZIP_BY_SUBSTREAM

    def initialize(fairy, others, block_source, opts=nil)
      super(fairy, others.collect{|o| o.backend}, block_source, opts)
      @others = others
      @block_source
      @opts = opts
p @opts
    end

    def backend_class_name
      "BZipper"
    end
  end
end

