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

#      @imports = Queue.new
    end

    def input=(input)
      super
      start
    end

#     def add_input(input)
#       unless input
# 	@imports.push nil
# 	return self
#       end
#       policy = @opts[:prequeuing_policy]
#       import = Import.new(policy)
#       import.add_key(input.key)
#       input.output = import
#       import.set_log_callback do |n| 
# 	Log::verbose(self, "IMPORT POP: #{n}")
#       end
#       @imports.push import
#       self
#     end

    def basic_start(&block)
      Log::debug(self, "START")
      output_uri = gen_real_file_name
      @vfile.set_real_file(no, output_uri)

      Log::debug(self, "write real file: #{output_uri}")
      begin
	output_file = URI.parse(output_uri).path
      rescue
	Log::debug_exception(self)
	raise
      end

      unless File.exist?(File.dirname(output_file))
	create_dir(File.dirname(output_file))
      end

      File.open(output_file, "w") do |io|
	Log::debug(self, "start write real file: #{output_uri}")
	@input.each do |l|
	  io.puts l
	end
	Log::debug(self, "finish write real file: #{output_uri}")
      end
      self.status = ST_OUTPUT_FINISH
    end

    def create_dir(path)
      unless File.exist?(File.dirname(path))
	create_dir(File.dirname(path))
      end
      begin
	Dir.mkdir(path)
      rescue Errno::EEXIST
	# 無視
      end
    end

    IPADDR_REGEXP = /::ffff:([0-9]+\.){3}[0-9]+|[0-9a-f]+:([0-9a-f]*:)[0-9a-f]*/

    def gen_real_file_name
      host= processor.addr
      root = CONF.VF_ROOT
      prefix = CONF.VF_PREFIX
      base_name = @vfile.base_name
      no = @input.no
      

      if IPADDR_REGEXP =~ host
	begin
	  host = Resolv.getname(host)
	rescue
	  # ホスト名が分からない場合 は そのまま ipv6 アドレスにする
	  host = "[#{host}]"
	end
      end
      
      format("file://#{host}#{root}/#{prefix}/#{base_name}-%03d", no)
    end
  end
end
