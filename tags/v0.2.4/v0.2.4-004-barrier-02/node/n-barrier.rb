
require "node/n-identity"


module Fairy
  class NBarrierMemoryBuffer<NIdentity

    def initialize(processor, bjob)
      @export = Export.new(Queue.new)
      super(processor, bjob)
    end

    def input=(input)
      unless @import
	@import = Import.new(Queue.new)
	@import.no=input.no
	@import.add_key(input.key)
	start
      end
      self
    end

  end

end
