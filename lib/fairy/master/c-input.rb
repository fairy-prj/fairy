# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-filter"

module Fairy
  class CInput<CFilter
    def initialize(*rests)
      super
    end

    def output=(output)
      @output = output
    end

#     def start
#       @create_node_thread = Thread.start {
# 	create_and_start_nodes
#       }
#     end
  end
end
