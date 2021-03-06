# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "e2mmap"

require "xthread"

require "fairy/share/conf.rb"

module Fairy
  class VFile
    VFILE_EXT = ".vf"
    VFILE_HEADER = "#!fairy vfile"
    VFILE_MAGIC = /^#{Regexp.escape(VFILE_HEADER)}/

    def VFile.vfile?(path)
      if File.extname(path) == VFILE_EXT
	return true
      end
      if !File.exist?(path)
	return false
      end
	
      File.open(path) do |io|
	l = io.gets
	begin
	  return VFILE_MAGIC =~ l
	rescue ArgumentError
	  return false
	end
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
      @real_file_names_cv = XThread::ConditionVariable.new

      @base_name = nil

      @BASE_NAME_CONVERTER = nil
      if CONF.VF_BASE_NAME_CONVERTER
	@BASE_NAME_CONVERTER = eval(CONF.VF_BASE_NAME_CONVERTER)
      end
    end

    attr_reader :vfile_name
    attr_reader :base_name
    
    def vfile_name=(path)
      @vfile_name = path
      #@base_name = File.dirname(path)+"/"+File.basename(path, VFILE_EXT)
      @base_name = File.expand_path(File.dirname(path))+"/"+File.basename(path, VFILE_EXT)

      if @BASE_NAME_CONVERTER
	@base_name = @BASE_NAME_CONVERTER.call(@base_name)
      end

      # 絶対パスの場合/を取る(取りあえずの処置)
      while @base_name.sub!(/^\//, ""); end

      # .. があったときの処理
      while @base_name.sub!(%r{/[^/]*/\.\./?}, ""); end
      while @base_name.sub!(/^\.\./, ""); end
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
	  ERR::Raise NoVFile, path
	end

	files = []
	for l in io
	  l.chomp!
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

#    VF_PREFIX = CONF.VF_PREFIX
#    IPADDR_REGEXP = /::ffff:([0-9]+\.){3}[0-9]+|[0-9a-f]+:([0-9a-f]*:)[0-9a-f]*/

#     # file name: #{base}-NNN
#     def gen_real_file_name(host, root, no)

#       if IPADDR_REGEXP =~ host
# 	begin
# 	  host = Resolv.getname(host)
# 	rescue
# 	  # ホスト名が分からない場合 は そのまま ipv6 アドレスにする
# 	  host = "[#{host}]"
# 	end
#       end
      
#       base = "file://#{host}#{root}/#{VF_PREFIX}/#{@base_name}"

#       base_regexp = /^#{Regexp.escape(base)}/
#       fn = nil
#       @real_file_names_mutex.synchronize do
# 	ary = @real_file_names.select{|e| base_regexp =~ e}.sort
# 	if ary.empty?
# 	  fn = "#{base}-000"
# 	else
# 	  fn = ary.last.succ
# 	end
# 	@real_file_names.push fn
#       end
#       fn
#     end

    def set_real_file(no, url)
      @real_file_names_mutex.synchronize do
	@real_file_names[no] = url
      end
    end

    # Ruby 1.9 mershal 対応
    #  - Ruby 1.9 では mutex を dump できない
    def marshal_dump
      [@vfile_name, @real_file_names]
    end

    def marshal_load(ary)
      @vfile_name = ary[0]
      @real_file_names = ary[1]
      @real_file_names_mutex = Mutex.new
      @real_file_names_cv = XThread::ConditionVariable.new
    end

  end
end
