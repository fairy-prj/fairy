
module Fairy
  class NInputVArray<NJob
    Processor.def_export self

    include NSingleExportable

    def initialize(processor, bjob, opts, array)
      super
      @array = array
    end

    def start
      super do
	for i in @array
	  @export.push i
	end
      end
    end

  end
end

