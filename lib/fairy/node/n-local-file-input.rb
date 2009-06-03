# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/n-single-exportable"

module Fairy
  class NLFileInput<NSingleExportInput
    Processor.def_export self

    def self.open(processor, bjob, io, opts=nil)
      nlfileinput = self.new(processor, bjob, opts)
      nlfileinput.open(job)
    end

    def initialize(processor, bjob, opts=nil)
      super
    end

    def open(io)
      @io = io
      start
      self
    end

    def start
      super do
	buf_size = @opts[:buffer_size]
	buf_size = CONF.LOCAL_INPUT_FILE_BUFFER_SIZE unless buf_size

	rest = nil
	while (buf = @io.read(buf_size))
	  lines = buf.scan(/.*\n?/)
	  lines.pop
	  if rest
	    begin
	      lines[0] = rest+lines[0]
	    rescue
	      Log::debug(self, "AAAAAAAAAAAAAAAA")
	      Log::debug(self, buf.inspect)
	      Log::debug(self, lines.inspect)
	      Log::debug(self, "N: 4")
	      raise
	    end
	  end
	  rest = lines.pop
	  if false && @export.respond_to?(:push_buf)
	    @export.push_buf lines
	  else
	    for l in lines
	      @export.push l
	    end
	  end
	end
	if rest
	  @export.push rest
	end
	@io.close
	@io = nil # FileオブジェクトをGCの対象にするため
      end
    end
  end
end
