
module Fairy
  class NIota<NJob
    Processor.def_export self

    include NSingleExportable

    def initialize(processor, bjob, opts, first, last)
      super
      @first = first
      @last = last
    end

    def start
      super do
	for i in @first..@last
	  @export.push i
	end
      end
    end

  end
end

