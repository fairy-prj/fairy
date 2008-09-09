
require "backend/bfile"
require "backend/b-each-element-mapper"
require "backend/b-each-substream-mapper"
require "backend/b-each-element-selector"
require "backend/bhere"
require "backend/b-group-by"
require "backend/b-zipper"

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

