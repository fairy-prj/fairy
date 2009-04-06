# encoding: UTF-8

require "uri"

require "fairy/node/n-filter"

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
      policy = @opts[:prequeuing_policy]
      import = Import.new(policy)
      import.add_key(input.key)
      input.output = import
      import.set_log_callback do |n| 
	Log::info(self, "IMPORT POP: #{n}")
      end
      @imports.push import
      self
    end

    def start
      Log::debug(self, "START")
      # この位置重要. これによって, ファイル生成のシリアライズ性を保証している
      output_uri = @vfile.gen_real_file_name(@processor.addr, CONF.VF_ROOT)
      Log::debug(self, "write real file: #{output_uri}")
      begin
	p output_uri
	output_file = URI.parse(output_uri).path
      rescue
	Log::debug_exception
	raise
      end

Log::debug(self, "AAAAAAAA:1")
      unless File.exist?(File.dirname(output_file))
	create_dir(File.dirname(output_file))
      end

Log::debug(self, "AAAAAAAA:2")
      super do
Log::debug(self, "AAAAAAAA:3")
	File.open(output_file, "w") do |io|
	  Log::debug(self, "start write real file: #{output_uri}")

	  while import = @imports.pop
Log::debug(self, "AAAAAAAA:5")
	    for l in import
	      io.puts l
	    end
	  end
	  Log::debug(self, "finish write real file: #{output_uri}")
	end
	self.status = ST_OUTPUT_FINISH
      end
    end

    def create_dir(path)
      unless File.exist?(File.dirname(path))
	create_dir(File.dirname(path))
      end
      Dir.mkdir(path)
    end
  end
end
