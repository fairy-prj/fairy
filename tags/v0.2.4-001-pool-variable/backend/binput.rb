
require "backend/bjob"

module Fairy
  class BInput<BJob
    def output=(output)
      @output = output
    end
  end
end
