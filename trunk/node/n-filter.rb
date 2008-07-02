
require "node/njob"
require "node/port"

module Fairy
  class NFilter<NJob

    ST_WAIT_IMPORT = :ST_WAIT_IMPORT

    def initialize(processor, bjob)
      super
      @import = nil
      @no_import = 0

      self.status=ST_WAIT_IMPORT
    end

    attr_reader :import

    def input=(input)
      unless @import
	@import = Import.new
	@import.add_key(input.key)
	start
      end
      self
    end
  end
end
