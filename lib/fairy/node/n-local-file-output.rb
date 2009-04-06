# encoding: UTF-8

require "uri"

require "fairy/node/n-single-exportable"

module Fairy
  class NLFileOutput<NSingleExportFilter
    Processor.def_export self

    ST_OUTPUT_FINISH = :ST_OUTPUT_FINISH

    def initialize(processor, bjob, opts=nil)
      super

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
      policy = @opts[:prequeuing_policy]
      import = Import.new(policy)
      import.no=(input.no)
      import.add_key(input.key)
      import.set_log_callback do |n| 
	Log::info(self, "IMPORT POP: #{n}")
      end

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
