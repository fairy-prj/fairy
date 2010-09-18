# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  class Inspector
    def initialize(obj)
      @obj = obj
    end

    def exec str
      @obj.instance_eval str
    end
  end
end
