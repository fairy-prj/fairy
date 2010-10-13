# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "thread"
require "forwardable"

require "fairy/share/conf.rb"

module Fairy
  class FastTempfile
    extend Forwardable

    Entry = Struct.new(:path, :io)

    def self.reset
      @DEFAULT_TMPDIR = CONF.TMP_DIR
      @PID = $$
      @HEAD = Time.now.strftime("%Y%m%d")+format("-%05d-", @PID)
      @Seq = "00000"
    end
    reset
    @Mutex = Mutex.new

    def self.open(prefix, tmpdir = @DEFAULT_TMPDIR)
      new(prefix, tmpdir)
    end

    def self.gen_tmpname(prefix, tmpdir)
      @Mutex.synchronize do
	# forkしたときのため
	reset if @PID != $$
	name = tmpdir+"/"+prefix+@HEAD+@Seq
	@Seq = @Seq.succ
	name
      end
    end

    def initialize(prefix, tmpdir)
      @entry = Entry.new
      ObjectSpace.define_finalizer(self, FastTempfile.terminate_proc(@entry))

      @entry.path = FastTempfile.gen_tmpname(prefix, tmpdir)

      begin
	@entry.io = File.open(path, File::RDWR|File::CREAT|File::EXCL)
      rescue Errno::ENOENT
	unless File.directory?(tmpdir)
	  ERR::Fail ERR::NoTmpDir, tmpdir
	end
	raise
      end
    end

    def_delegator :@entry, :path
    def_delegator :@entry, :io

    def open
      @entry.io.close if @entry.io
      @entry.io = File.open(path)
    end

    def close
      @entry.io.close
      @entry.io = nil
    end

    def close!
      @entry.io.close if @entry.io
      if File.exist?(@entry.path)
	File.unlink @entry.path
      end
      ObjectSpace.undefine_finalizer(self)
    end

    def self.terminate_proc(entry)
      pid = @PID
      Proc.new {
	if pid == $$
	  entry.io.close if entry.io

	  # keep this order for thread safeness
	  if entry.path
	    File.unlink(entry.path) if File.exist?(entry.path)
	  end
	end
      }
    end
  end
end


