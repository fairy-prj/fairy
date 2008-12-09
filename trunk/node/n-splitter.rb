
require "node/n-filter"

module Fairy
  class NSplitter<NFilter
    Processor.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(processor, bjob, opts, n)
      super
      @no_split = n

      @exports = @no_split.times.collect{Export.new}
    end

    attr_reader :exports

    def start
      super do
	begin
	  @import.each_slice(@no_split) do |ll|
	    if ll.size < @no_split
	      ll.fill(0, @no_split){|idx| ll[idx] ||= END_OF_STREAM}
	    end
	    @exports.zip(ll) do |exp, l|
	      exp.push l
	    end
	  end
	ensure
	  @exports.each{|exp| exp.push END_OF_STREAM}
	end
      end
    end
  end
end
