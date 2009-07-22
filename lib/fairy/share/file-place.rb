# encoding: UTF-8

require "thread"
require "uri"

module Fairy
  URI_REGEXP = /:\/\//

  class BFilePlace
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
	  fp = NFilePlace.new(file, @no)
	  @no += 1
	end
	block.call fp
      end
    end

  end

  class NFilePlace
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

  class BLocalIOPlace
    def initialize(job)
      @job = job
      @no = 0
    end

    def each_assigned_filter(&block)
Log::debug(self, "EACH_ASSIGNED_FILTER: S")
      @job.each_assigned_filter do |io|
Log::debug(self, "EACH_ASSIGNED_FILTER: 1")
	block.call NLocalIOPlace.new(io, @no)
Log::debug(self, "EACH_ASSIGNED_FILTER: 2")
	@no += 1
      end
Log::debug(self, "EACH_ASSIGNED_FILTER: E")
    end
  end

  class NLocalIOPlace
    def initialize(io, no)
      @io = io
      @no = no
    end

    attr_reader :no
    attr_reader :io
  end

end
