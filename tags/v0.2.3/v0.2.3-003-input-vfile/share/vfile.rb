
module Fairy

  VFILE_MAGIC = /^\#\!fairy vfile/
  
  class VFile

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
      @real_file_names = nil
    end

    attr_accessor :real_file_names

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

	@real_file_names = files
      end
    end

    def each_real_file_name(&block)
      @real_file_names.each &block
    end
    alias each each_real_file_name
  end
end
