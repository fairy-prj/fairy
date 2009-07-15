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

#     def next_filter(mapper)
#       @nfileplaces_mutex.synchronize do
# 	if file = @nfileplaces.shift
# 	  fp = NFilePlace.new(file, @no)
# 	  @no += 1
# 	  fp
# 	else
# 	  nil
# 	end
#       end

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

    attr_reader :no
    attr_reader :host
    attr_reader :path

  end
end
