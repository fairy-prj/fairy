# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"

module Fairy
  class PSegSplit<PIOFilter
    Processor.def_export self

#    DeepConnect.def_single_method_spec(self, "REF new(REF, VAL, VAL)")

    def initialize(id, ntask, bjob, opts, n)
      super
      @no_split = n

      policy = @opts[:postqueuing_policy]
      @exports = @no_split.times.collect{Export.new(policy)}
    end

    attr_reader :exports
    DeepConnect.def_method_spec(self, "VAL exports")

    def start_export
      Log::debug(self, "START_EXPORT")
#      return unless @status == ST_WAIT_IMPORT
      start do
	begin
	  @input.each_slice(@no_split) do |ll|
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

#     def start
#       super do
# 	begin
# 	  @import.each_slice(@no_split) do |ll|
# 	    if ll.size < @no_split
# 	      ll.fill(0, @no_split){|idx| ll[idx] ||= END_OF_STREAM}
# 	    end
# 	    @exports.zip(ll) do |exp, l|
# 	      exp.push l
# 	    end
# 	  end
# 	ensure
# 	  @exports.each{|exp| exp.push END_OF_STREAM}
# 	end
#       end
#     end
  end
end
