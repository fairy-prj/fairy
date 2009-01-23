
require "node/njob"
require "node/port"
require "node/n-single-exportable"

module Fairy
  class NFile<NJob
    include NSingleExportable

    def NFile.open(processor, bjob, fn, opts=nil)
      nfile = NFile.new(processor, bjob, opts)
      nfile.open(fn)
    end

    def initialize(processor, bjob, opts=nil)
      super
      @file = nil
    end

    def open(file_name)
      @file_name = file_name
      @file = File.open(file_name)
      start
      self
    end

    def start
      super do
	for l in @file
	  @export.push l
	end
	@file.close
	@file = nil # FileオブジェクトをGCの対象にするため
      end
    end
  end
end
