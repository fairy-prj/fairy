
module Fairy
  class VFile

    VFILE_HEADER = "#!fairy vfile"
    VFILE_MAGIC = /^#{Regexp.escape(VFILE_HEADER)}/

    def VFile.vfile?(path)
      File.open(path) do |io|
	l = io.gets
	return VFILE_MAGIC =~ l
      end
    end

    def VFile.vfile(path)
      vfile = new
      vfile.vfile(path)
      vfile
    end

    def VFile.real_files(real_files)
      vfile = new
      vfile.real_file_names = real_files
      vfile
    end
    
    def initialize
      @vfile_name = nil

      @real_file_names = []
      @real_file_names_mutex = Mutex.new
      @real_file_names_cv = ConditionVariable.new
    end

    attr_reader :vfile_name
    
    def vfile_name=(path)
      @vfile_name = path
      @base_name = path.gsub(/\//, "-")
    end

    def real_file_names
      @real_file_names_mutex.synchronize do
	@real_file_names
      end
    end


    def real_file_names=(val)
      @real_file_names_mutex.synchronize do
	@real_file_names=val
      end
    end

    def vfile(path)
      File.open(path) do |io|
	l = io.gets
	unless VFILE_MAGIC =~ l
	  raise "VFileではありません(#{path})"
	end

	files = []
	for l in io
	  next if l =~ /^\s*$/
	  next if l =~ /^\s*#.*$/
	  files.push l
	end
	
	@real_file_names_mutex.synchronize do
	  @real_file_names = files
	end
      end
    end

    def each_real_file_name(&block)
      real_file_names.dup.each &block
    end
    alias each each_real_file_name

    def create_vfile
      File.open(@vfile_name, "w") do |io|
	io.puts VFILE_HEADER
	io.puts
	@real_file_names_mutex.synchronize do
	  @real_file_names.each do |fn|
	    io.puts fn
	  end
	end
      end
    end

    TOP = "/tmp/fairy"

    IPADDR_REGEXP = /::ffff:([0-9]+\.){3}[0-9]+|[0-9a-f]+:([0-9a-f]*:)[0-9a-f]*/

    # file name: #{base}-NNN
    def gen_real_file_name(host)

      if IPADDR_REGEXP =~ host
	begin
	  host = Resolv.getname(host)
	rescue
	  # ホスト名が分からない場合 は そのまま ipv6 アドレスにする
	  host = "[#{host}]"
	end
      end

      base = "file://#{host}#{TOP}/#{@base_name}"
      base_regexp = /^#{Regexp.escape(base)}/
      fn = nil
      @real_file_names_mutex.synchronize do
	ary = @real_file_names.select{|e| base_regexp =~ e}.sort
	if ary.empty?
	  fn = "#{base}-000"
	else
	  fn = ary.last.succ
	end
	@real_file_names.push fn
      end
      fn
    end
  end
end
