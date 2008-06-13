
require "node/njob"
require "node/port"

module Fairy
  class NFilter<NJob

    ST_WAIT_IMPORT = :ST_WAIT_IMPORT

    def initialize(bjob)
      super
      @import = nil
      self.status=ST_WAIT_IMPORT
    end

    attr_reader :import

    def input=(input)
      @import = Import.new
      start
      self
    end
  end
end
