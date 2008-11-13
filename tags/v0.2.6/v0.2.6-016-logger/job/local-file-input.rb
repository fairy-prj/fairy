
require "job/job"
require "share/vfile"

module Fairy
  class LFileInput < Job

    def self.input(fairy, opts, filename)
      self.start(fairy, opts, filename)
    end

    def self.start(fairy, opts, filename)
      lfile = new(fairy, opts)
      lfile.start(filename)
      lfile
    end

    def initialize(fairy, opts=nil)
      super
      @io = nil
    end

    attr_reader :io

    def backend_class_name
      "BLFileInput"
    end

    def start(filename)
      @filename = filename
      backend.start(self)
    end

    def open
      if block_given?
	io = File.open(@filename)
	begin
	  yield io
	ensure
	  io.close
	end
      else
	File.open(@filename)
      end
    end

    def split_opens(split_size, &block)
      begin
	seek = 0
	size = File.stat(@filename).size
	while seek < size
	  io = SplittedFile.open(@filename, seek, seek += split_size)
	  yield io
	end
      rescue
	Log::warn_exception(self, $!)
	raise
      end
    end

    class SplittedFile
      def self.open(fd, seek_start, seek_end, &block)
	sf = new(fd, seek_start, seek_end)
	if block_given?
	  begin
	    yield sf
	  ensure
	    sf.close
	  end
	else
	  sf
	end
      end

      def initialize(fd, seek_start, seek_end)

	@io = File.open(fd)
	@seek_start = seek_start
	@seek_end = seek_end

	if seek_start > 0
	  @io.seek(seek_start-1)
	  if /^$/ !~ @io.read(1)
	    # 一行空読みする
	    @io.gets
	  end
	end
	@io
      end

      def close
	@io.close
	@io = nil
      end

      def each(&block)
	begin
	  while @io.pos < @seek_end && l = @io.gets
	    yield l
	  end
	rescue
	  Log::warn_exception(self, $!)
	  raise
	end
      end
    end
  end
end
