
require "uri"

require "node/n-filter"

module Fairy
  class NFileOutput<NFilter
    
    ST_OUTPUT_FINISH = :ST_OUTPUT_FINISH

    def initialize(processor, bjob, vf)
      super(processor, bjob)
      @vfile = vf
    end

    def start
      output_uri = @vfile.gen_real_file_name(@processor.addr)
      output_file = URI.parse(output_uri).path

      super do
	File.open(output_file, "w") do |io|
	  for l in @import
	    io.puts l
	  end
	end
	self.status = ST_OUTPUT_FINISH

      end
      

    end
  end

end
