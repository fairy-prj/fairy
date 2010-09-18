# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  class Scheduler

    def initialize(controller)
      # 取りあえず
      @controller = controller
      #@processors = [Processor.new]
    end
      

    def schedule(atom)
      # 取りあえず.
      # ここで, atomにはノード指定がすでにあるとしている.
      # 指定されたノードにスケジュールする.
      @processors[0].exec atom
    end
    
  end
end
