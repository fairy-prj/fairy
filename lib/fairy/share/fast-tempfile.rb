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
Log::debug(self, "INITIALIZE: S")
      @Mutex.synchronize do
	# forkしたときのため
Log::debug(self, "INITIALIZE: 1")
	reset if @PID != $$
Log::debug(self, "INITIALIZE: 2")
	name = tmpdir+"/"+prefix+@HEAD+@Seq
Log::debug(self, "INITIALIZE: 3")
	@Seq = @Seq.succ
Log::debug(self, "INITIALIZE: E")
	name
      end
    end

    def initialize(prefix, tmpdir)
Log::debug(self, "INITIALIZE: S")
      @entry = Entry.new
Log::debug(self, "INITIALIZE: 1")
      ObjectSpace.define_finalizer(self, FastTempfile.terminate_proc(@entry))
Log::debug(self, "INITIALIZE: 2")

      @entry.path = FastTempfile.gen_tmpname(prefix, tmpdir)
Log::debug(self, "INITIALIZE: 3")
      @entry.io = File.open(path, File::RDWR|File::CREAT|File::EXCL)
Log::debug(self, "INITIALIZE: S")
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


