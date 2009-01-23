
require "backend/bfile"
require "backend/b-each-element-mapper"

module Fairy
  class JobInterpriter
    def initialize(controller)
      @controller = controller
    end

    def exec(atom)
      puts "SEND: #{atom.receiver} #{atom.message}, #{atom.args.map{|e| e.to_s}.join(",")}"
      ret = atom.receiver.send(atom.message, *atom.args)
      # このあと何か必要か?
    end
  end
end

