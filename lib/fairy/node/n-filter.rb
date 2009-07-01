# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/port"

module Fairy
  class NFilter<NJob
    Processor.def_export self

    ST_WAIT_IMPORT = :ST_WAIT_IMPORT

    def initialize(processor, bjob, opts=nil, *rests)
      super
      @import = nil

      self.status=ST_WAIT_IMPORT
    end

    attr_reader :import

    def input=(input)
      @input = input
      self.no = input.no
#       unless @import

# 	policy = @opts[:prequeuing_policy]

# 	self.no = input.no
# 	@import = Import.new(policy)
# 	@import.no=input.no
# 	@import.add_key(input.key)
# 	@import.set_log_callback do |n| 
# 	  Log::verbose(self, "IMPORT POP: #{n}")
# 	end
# 	start
#       end
      self
    end
  end
end
