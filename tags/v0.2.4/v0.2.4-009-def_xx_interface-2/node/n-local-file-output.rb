
require "uri"

require "node/n-filter"

module Fairy
  class NLFileOutput<NFilter
    include NSingleExportable
    
    ST_OUTPUT_FINISH = :ST_OUTPUT_FINISH

    def initialize(processor, bjob, opts=nil)
      super(processor, bjob, opts)

      @imports = Queue.new
    end

    def input=(input)
      super
      @imports.push @import
    end

    def add_input(input)
      unless input
	@imports.push nil
	return self
      end
      import = Import.new
      import.no=(input.no)
      import.add_key(input.key)
      input.output = import
      @imports.push import
      self
    end

    def start
      super do
	while import = @imports.pop
	  for l in import
	    @export.push l
	  end
	end
      end
    end
  end
end
