# encoding: UTF-8

require "fairy/backend/bfile"
require "fairy/backend/b-each-element-mapper"
require "fairy/backend/b-each-substream-mapper"
require "fairy/backend/b-each-element-selector"
require "fairy/backend/bhere"
require "fairy/backend/b-group-by"
require "fairy/backend/b-zipper"

module Fairy
  class JobInterpriter
    def initialize(controller)
      @controller = controller
    end

    def exec(atom)
      puts "SEND: #{atom.receiver}.#{atom.message}(#{atom.args.map{|e| e.to_s}.join(",")})"
      ret = atom.receiver.send(atom.message, *atom.args)
      # このあと何か必要か?
    end
  end
end

