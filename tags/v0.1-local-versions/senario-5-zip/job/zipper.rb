
require "job/filter"

module Fairy
  class Zipper<Filter
    ZIP_BY_SUBSTREAM = :ZIP_BY_SUBSTREAM

    def initialize(fairy, opts, others, block_source)
      super
      @opts = opts
      @others = others
      @block_source
    end

    def backend_class
      BZipper
    end
  end
end

class BZipper;end

