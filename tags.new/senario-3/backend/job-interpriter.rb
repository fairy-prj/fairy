
require "backend/bfile"
require "backend/b-each-element-mapper"
require "backend/bhere"

module Fairy
  class JobInterpriter
    def initialize(controller)
      @controller = controller
    end

    def exec(atom)
      puts "SEND: #{atom.receiver}.#{atom.message}(#{atom.args.map{|e| e.to_s}.join(",")})"
      ret = atom.receiver.send(atom.message, *atom.args)
      # ���Τ��Ȳ���ɬ�פ�?
    end
  end
end

