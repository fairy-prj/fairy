
require "job/filter"

module Fairy
  class Zipper<Filter
    ZIP_BY_SUBSTREAM = :ZIP_BY_SUBSTREAM

    def initialize(fairy, opts, others, block_source)
      super(fairy, opts, others.collect{|o| o.backend}, block_source)
      @opts = opts
      @others = others
      @block_source
    end

    def backend_class_name
      "BZipper"
    end
  end
end

