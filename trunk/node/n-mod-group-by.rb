
require "node/n-filter"
require "node/n-group-by"

module Fairy
  class NModGroupBy<NGroupBy
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      
      @mod = CONF.N_MOD_GROUP_BY
    end

    def key(e)
      super.hash % @mod
    end
  end

  class NPreAfterModFilter<NSingleExportFilter
    Processor.def_export self

    def start
      super do
	@import.each{|e| @export.push e}
      end
    end

  end

  class NPostAfterModFilter<NSingleExportFilter
    Processor.def_export self

    def start
      super do
	@import.each{|e| @export.push e}
      end
    end

  end

end


