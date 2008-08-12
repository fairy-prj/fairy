
require "node/n-filter"

module Fairy
  class NSplitter<NFilter

    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(processor, bjob, n, opts=nil)
      super(processor, bjob)
      @no_split = n
      @opts = opts

      @exports = @no_split.times.collect{Export.new}
    end

    attr_reader :exports

    def start
      super do
	@import.each_slice(@no_split) do |ll|
	  if ll.size < @no_split
	    ll.fill(0, @no_split){|idx| ll[idx] ||= END_OF_STREAM}
	  end
	  @exports.zip(ll) do |exp, l|
	    exp.push l
	  end
	end
	@exports.each{|exp| exp.push END_OF_STREAM}
      end
    end
  end
end
