
require "node/njob"
require "node/port"

module Fairy
  class NFilter<NJob
    def initialize
      super
      @import = nil
    end

    attr_reader :import

    def input=(input)
      @import = Import.new
      start
      self
    end

    def start
      raise "start ���������Ƥ��ʤ�"
    end
  end
end
