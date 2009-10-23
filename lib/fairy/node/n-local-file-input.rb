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

    def open(nioplace)
      @io = nioplace.io
      self.no = nioplace.no

      @buffer_size = @opts[:buffer_size]
      @buffer_size = CONF.LOCAL_INPUT_FILE_BUFFER_SIZE unless @buffer_size
    end

    def basic_each(&block)
      rest = nil
      while (buf = @io.read(@buffer_size))
	lines = buf.scan(/.*\n?/)
	lines.pop # scan で末尾にゴミが出るため
	if rest
	  begin
	    lines[0] = rest+lines[0]
	  rescue
	    Log::debug(self, @io.inspect)
	    Log::debug(self, buf.inspect)
	    Log::debug(self, lines.inspect)
	    Log::debug(self, rest.inspect)
	    raise
	  end
	end
	rest = lines.pop
	lines.each &block
      end
      if rest
	block.call rest
      end
      @io.close
      @io = nil # FileオブジェクトをGCの対象にするため
    end
   
  end
end
