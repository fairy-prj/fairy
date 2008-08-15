
require "uri"

require "node/n-filter"

module Fairy
  class NFileOutput<NFilter
    Processor.def_export self
    
    ST_OUTPUT_FINISH = :ST_OUTPUT_FINISH

    def initialize(processor, bjob, opt, vf)
      super
      @vfile = vf

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
      import.add_key(input.key)
      input.output = import
      @imports.push import
      self
    end

    def start
      output_uri = @vfile.gen_real_file_name(@processor.addr)
      output_file = URI.parse(output_uri).path

      super do
	File.open(output_file, "w") do |io|
	  while import = @imports.pop
	    for l in import
	      io.puts l
	    end
	  end
	end
	self.status = ST_OUTPUT_FINISH
      end
    end
  end
end
