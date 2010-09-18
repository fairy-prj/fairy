# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "thread"
require "uri"

module Fairy
  URI_REGEXP = /:\/\//

  class CFilePlace
    def initialize(vfile)
      @vfile = vfile

      # for next_filter
      @no = 0
      @nfileplaces = @vfile.real_file_names
      @nfileplaces_mutex = Mutex.new
    end

    def each_assigned_filter(&block)
      loop do
	fp = nil
	@nfileplaces_mutex.synchronize do
	  file = @nfileplaces.shift
	  return unless file
	  fp = PFilePlace.new(file, @no)
	  @no += 1
	end
	block.call fp
      end
    end

  end

  class PFilePlace
    def initialize(url, no)
      @url = url
      @no = no

      @host = "localhost"
      @path = @url
      if URI_REGEXP =~ @url
	uri = URI(@url)
	@host = uri.host
	if /^\[([0-9a-f.:]*)\]$/ =~ @host
	  @host = $1
	end
	@path = uri.path
      end
    end

    attr_reader :url
    attr_reader :no
    attr_reader :host
    attr_reader :path
  end

  class CLocalIOPlace
    def initialize(job)
      @job = job
      @no = 0
    end

    def each_assigned_filter(&block)
      @job.each_assigned_filter do |io|
	block.call PLocalIOPlace.new(io, @no)
	@no += 1
      end
    end
  end

  class PLocalIOPlace
    def initialize(io, no)
      @io = io
      @no = no
    end

    attr_reader :no
    attr_reader :io
  end

  class CVarrayPlace
    def initialize(varray)
      @varray = varray
      @no = 0
    end

    def each_assigned_filter(&block)
      no = 0
      @varray.arrays_each do |ary|
	vp = PVarrayPlace.new(ary, no)
	no += 1
	block.call vp
      end
    end
  end

  class PVarrayPlace
    def initialize(ary, no)
      @ary = ary
      @no = no

      @host = "localhost"
      @path = @url
      if URI_REGEXP =~ @url
	uri = URI(@url)
	@host = uri.host
	if /^\[([0-9a-f.:]*)\]$/ =~ @host
	  @host = $1
	end
	@path = uri.path
      end
    end

    attr_reader :ary
    attr_reader :no
  end

  class CIotaPlace
    def initialize(last, offset, split_no)
      @last = last
      @offset = offset
      @split_no = split_no
    end

    def each_assigned_filter(&block)
      first = @offset
      no = -1

      @split_no.times do
	no += 1
	Log::debug self, "NO: #{no}"
	last = [first + @last.div(@split_no), @last].min
	block.call PIotaPlace.new(no, first, last)
	first = last + 1
	break if first > @last
      end
#      sleep 0.1
    end
  end

  class PIotaPlace
    def initialize(no, first, last)
      @no = no
      @first = first
      @last = last
    end

    attr_reader :no
    attr_reader :first
    attr_reader :last
  end


  class CTherePlace
    def initialize(enum)
      @enumerable = enum
    end

    def each_assigned_filter(&block)
      block.call PTherePlace.new(0, @enumerable)
    end
  end

  class PTherePlace
    def initialize(no, enum)
      @no = no
      @enumerable = enum
    end

    attr_reader :no
    attr_reader :enumerable
  end
end
