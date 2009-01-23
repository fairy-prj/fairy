
require "backend/bjob"

module Fairy
  class BInput<BJob
    def output=(output)
      @output = output
    end

    def break_running(njob=nil)
      # 取りあえずむし
    end

    def break_creat_node
      # 取りあえずむし
    end

  end
end
