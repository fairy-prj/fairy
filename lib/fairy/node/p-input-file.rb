# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-filter"
require "fairy/node/p-single-exportable"

require "fairy/share/file-place"

module Fairy
  class PInputFile<PSingleExportInput
    Processor.def_export self

    def PInputFile.open(processor, bjob, opts, fn)
      nfile = PInputFile.new(processor, bjob, opts)
      nfile.open(fn)
    end

    def initialize(id, ntask, bjob, opts=nil)
      super
      @file = nil
    end

    def open(nfileplace)
      @file_name = nfileplace.path
      self.no = nfileplace.no
      begin
	@file = File.open(@file_name)
      rescue 
	e = $!.exception($!.message+ "(vfile entry##{nfileplace.no}: #{nfileplace.url})")
	e.set_backtrace($!.backtrace)

	Log::error_exception(e)
	handle_exception(e)
	raise e
      end
#      start
      self
    end
    DeepConnect::def_method_spec(self, "REF open(VAL)")

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

#     def start
#       super do
# 	for l in @file
# 	  @export.push l
# 	end
# 	@file.close
# 	@file = nil # FileオブジェクトをGCの対象にするため
#       end
#     end

    def basic_each(&block)
      begin
	@file.each &block
      ensure
	@file.close
	@file = nil # FileオブジェクトをGCの対象にするため
      end
    end

    def basic_next
      begin
	ret = @file.gets
      ensure
	unless ret
	  @file.close
	  @file = nil
	  return :END_OF_STREAM 
	end
      end
    end
  end
end
