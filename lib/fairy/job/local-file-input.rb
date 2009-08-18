# encoding: UTF-8

require "fairy/job/job"
require "fairy/share/vfile"

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

    def each_assigned_filter(&block)
Log::debug(self, "EACH_ASSIGNED_FILTER: S")
      if !@opts[:split_size]
Log::debug(self, "EACH_ASSIGNED_FILTER: 1")
	each_assigned_filter1(&block)
      else
Log::debug(self, "EACH_ASSIGNED_FILTER: 2")
	each_assigned_filer_split(&block)
      end
    end

    def each_assigned_filter1(&block)
      io = File.open(@filename)
      yield io
    end

    def each_assigned_filer_split(&block)
      split_size = @opts[:split_size]
      begin
	seek = 0
	size = File.stat(@filename).size
	while seek < size
	  io = SplittedFile.open(@filename, seek, seek + split_size)
	  seek = io.seek_end + 1
	  yield io
	end
      rescue
	Log::warn_exception(self)
	raise
      end
      nil
    end

#     def open
#       if block_given?
# 	io = File.open(@filename)
# 	begin
# 	  yield io
# 	ensure
# 	  io.close
# 	end
#       else
# 	File.open(@filename)
#       end
#     end

#     def split_opens(split_size, &block)
#       begin
# 	seek = 0
# 	size = File.stat(@filename).size
# 	while seek < size
# 	  io = SplittedFile.open(@filename, seek, seek + split_size)
# 	  seek = io.seek_end + 1
# 	  yield io
# 	end
#       rescue
# 	Log::warn_exception(self)
# 	raise
#       end
#     end

    class SplittedFile
      include Enumerable

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

	@io.seek(seek_end)
	c = @io.read(1)
	case c
	when nil, "\n"
	  @seek_end = seek_end
	else
	  @io.gets
	  @seek_end = @io.pos - 1
	end
	@io.seek(seek_start)
      end

      attr_reader :seek_start
      attr_reader :seek_end

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
	  Log::warn_exception(self)
	  raise
	end
      end

      def read(length)
	if @seek_end - @io.pos + 1 < length
	  length = @seek_end - @io.pos + 1
	end
	if length == 0
	  nil
	else
	  @io.read(length)
	end
      end
    end
  end
end
