# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/port"
require "fairy/node/n-single-exportable"

module Fairy
  class NFile<NSingleExportInput
    Processor.def_export self

    def NFile.open(processor, bjob, opts, fn)
      nfile = NFile.new(processor, bjob, opts)
      nfile.open(fn)
    end

    def initialize(processor, bjob, opts=nil)
      super
      @file = nil
    end

    def open(file_name)
      @file_name = file_name
      @file = File.open(file_name)
      start
      self
    end

#     def start
#       buf = ""
#       buf_size = 1024*20
#       rest = nil
#       super do
# 	while @file.read(buf_size, buf)
# 	  lines = buf.split
# 	  if rest
# 	    begin
# 	      lines[0] = rest+lines[0]
# 	    rescue
# 	      Log::debug(self, "AAAAAAAAAAAAAAAA")
# 	      Log::debug(self, buf.inspect)
# 	      Log::debug(self, lines.inspect)
# 	      raise
# 	    end
# 	  end
# 	  rest = lines.pop
# 	  if @export.respond_to?(:push_buf)
# 	    @export.push_buf lines
# 	  else
# 	    for l in lines
# 	      @export.push l
# 	    end
# 	  end
# 	end
# 	if rest
# 	  @export.push rest
# 	end
# 	@file.close
# 	@file = nil # FileオブジェクトをGCの対象にするため
#       end
#     end

    def start
      super do
	for l in @file
	  @export.push l
	end
	@file.close
	@file = nil # FileオブジェクトをGCの対象にするため
      end
    end
  end
end
