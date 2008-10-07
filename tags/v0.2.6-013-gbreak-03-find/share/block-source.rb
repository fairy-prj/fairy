
module Fairy
  class BlockSource
    def initialize(source)
      @source = source
      @backtrace = caller(1).select{|l| /fairy.*(share|job)/ !~ l}
      l = caller(1)[caller(1).index(backtrace.first)-1]
      @caller_method = (/in `(.*)'/.match(l))[1]
    end

    attr_reader :source
    attr_reader :backtrace
    attr_reader :caller_method
  end
end

