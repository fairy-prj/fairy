# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/port"
require "fairy/node/n-single-exportable"

module Fairy
  class NFile<NSingleExportInput
    Processor.def_export self

    def NFile.open(processor, bjob, opts, fn)
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
