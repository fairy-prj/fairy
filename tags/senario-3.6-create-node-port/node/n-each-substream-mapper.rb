
require "node/njob"
require "node/port"

module Fairy
  class NEachSubStreamMapper<NFilter1to1
    def initialize(bjob, block_source)
      super(bjob)
      @block_source = block_source
      @map_proc = eval("proc{#{@block_source}}", TOPLEVEL_BINDING)
    end

    def start
      super do
	@map_proc.call(@import, @export)
      end
    end
  end

end
